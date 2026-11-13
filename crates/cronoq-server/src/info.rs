//! Bounded cache of task details for `Get` and for result messages.
//!
//! The scheduler engine only answers "what is the status of task X"; this cache adds the
//! pool, key, kind, attempt counts, timestamps and the last error message. It is filled when
//! tasks are submitted through this server and from the scheduler's event stream, and evicts
//! the oldest records beyond its capacity.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use cronoq::{Priority, StatusEvent, TaskStatus};
use rustc_hash::FxHashMap;

/// What is known about one task.
#[derive(Clone, Debug, Default)]
pub struct TaskMeta {
    /// Pool name.
    pub pool: Arc<str>,
    /// Unique key, if any.
    pub key: Option<Arc<str>>,
    /// Kind, if submitted through this server.
    pub kind: Option<Arc<str>>,
    /// Attempts allowed (0 if unknown).
    pub max_attempts: u32,
    /// Last observed status.
    pub status: Option<TaskStatus>,
    /// Last observed attempt number.
    pub attempt: u32,
    /// Submission time (Unix ns).
    pub created_ns: Option<i64>,
    /// Start of the latest attempt (Unix ns).
    pub started_ns: Option<i64>,
    /// Terminal time (Unix ns).
    pub finished_ns: Option<i64>,
    /// Last error or status message.
    pub message: Option<Arc<str>>,
    /// Tenant, if submitted through this server.
    pub tenant: Option<Arc<str>>,
    /// Priority, if submitted through this server.
    pub priority: Option<Priority>,
}

#[derive(Default)]
struct Inner {
    /// Details of submissions stored to run after a task that has started, by that task's
    /// id, until its successor shows up or it ends without one.
    stored: FxHashMap<u64, TaskMeta>,
    map: FxHashMap<u64, TaskMeta>,
    // Keys come from callers: std's seeded SipHash, not FxHash, so they cannot be made to collide.
    by_key: std::collections::HashMap<Arc<str>, u64>,
    order: VecDeque<u64>,
}

/// Bounded map of task id to [`TaskMeta`].
pub struct TaskInfoCache {
    inner: Mutex<Inner>,
    cap: usize,
}

impl TaskInfoCache {
    /// Cache holding at most `cap` tasks.
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(TaskInfoCache {
            inner: Mutex::new(Inner::default()),
            cap,
        })
    }

    fn evict(st: &mut Inner, cap: usize) {
        while st.map.len() > cap {
            let Some(id) = st.order.pop_front() else {
                break;
            };
            if let Some(m) = st.map.remove(&id)
                && let Some(k) = m.key
                && st.by_key.get(&k) == Some(&id)
            {
                st.by_key.remove(&k);
            }
        }
    }

    fn entry<'a>(st: &'a mut Inner, id: u64, pool: &Arc<str>) -> &'a mut TaskMeta {
        if !st.map.contains_key(&id) {
            st.order.push_back(id);
        }
        st.map.entry(id).or_insert_with(|| TaskMeta {
            pool: pool.clone(),
            ..Default::default()
        })
    }

    /// Record a submission (not for duplicates: the original details stay).
    pub fn on_submit(&self, id: u64, meta: TaskMeta) {
        if self.cap == 0 {
            return;
        }
        let mut st = self.inner.lock().unwrap();
        if let Some(k) = &meta.key {
            st.by_key.insert(k.clone(), id);
        }
        let pool = meta.pool.clone();
        let e = Self::entry(&mut st, id, &pool);
        let (status, attempt, started, finished, message) = (
            e.status,
            e.attempt,
            e.started_ns,
            e.finished_ns,
            e.message.clone(),
        );
        *e = TaskMeta {
            status,
            attempt,
            started_ns: started,
            finished_ns: finished,
            message,
            ..meta
        };
        Self::evict(&mut st, self.cap);
    }

    /// Record that task `id` was replaced before it started: it keeps what happened to it so
    /// far and takes the description of the new submission (and its time, if `renewed`).
    pub fn on_replace(&self, id: u64, meta: TaskMeta, renewed: bool) {
        if self.cap == 0 {
            return;
        }
        let created = if renewed {
            meta.created_ns
        } else {
            self.get(id).and_then(|m| m.created_ns).or(meta.created_ns)
        };
        self.on_submit(
            id,
            TaskMeta {
                created_ns: created,
                ..meta
            },
        );
    }

    /// Record a submission stored to run after task `id`, which holds its key: its details
    /// become the successor's when that task appears.
    pub fn on_pending(&self, id: u64, meta: TaskMeta) {
        if self.cap == 0 {
            return;
        }
        let mut st = self.inner.lock().unwrap();
        let ended = st
            .map
            .get(&id)
            .and_then(|m| m.status)
            .is_some_and(|s| s.is_terminal());
        if ended {
            // The task ended before this was recorded, so its successor is already here
            // under the key: give it the details directly.
            let successor = meta.key.as_ref().and_then(|k| st.by_key.get(k).copied());
            if let Some(m) = successor
                .filter(|s| *s != id)
                .and_then(|s| st.map.get_mut(&s))
            {
                Self::describe(m, meta);
            }
            return;
        }
        // Bounded like the rest of the cache; an entry that is not kept only costs the
        // successor some details.
        if st.stored.len() >= self.cap {
            // Forget entries of tasks this cache no longer follows.
            let Inner { stored, map, .. } = &mut *st;
            stored.retain(|id, _| {
                map.get(id)
                    .is_some_and(|m| !m.status.is_some_and(|s| s.is_terminal()))
            });
        }
        if st.stored.len() < self.cap || st.stored.contains_key(&id) {
            st.stored.insert(id, meta);
        }
    }

    /// Give `m` the description of the submission it was created from, keeping what has
    /// happened to it.
    fn describe(m: &mut TaskMeta, from: TaskMeta) {
        m.pool = from.pool;
        m.key = from.key;
        m.kind = from.kind;
        m.max_attempts = from.max_attempts;
        m.tenant = from.tenant;
        m.priority = from.priority.or(m.priority);
    }

    /// Record a status event of a task in `pool`, observed at `at_ns` (Unix ns).
    pub fn on_event(&self, ev: &StatusEvent, pool: &Arc<str>, at_ns: i64) {
        if self.cap == 0 {
            return;
        }
        let id = ev.id.0;
        let mut st = self.inner.lock().unwrap();
        if let Some(k) = &ev.key {
            if ev.kind == cronoq::EventKind::Successor {
                // The key names its newest task.
                st.by_key.insert(k.clone(), id);
            } else {
                st.by_key.entry(k.clone()).or_insert(id);
            }
        }
        // A task that ends without a successor (cancelled, shed) takes the submission stored
        // behind it along; a successor takes its details.
        if ev.status.is_terminal()
            && !matches!(
                ev.status,
                TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::DeadLettered
            )
        {
            st.stored.remove(&id);
        }
        let stored = ev.follows().and_then(|old| st.stored.remove(&old.0));
        let e = Self::entry(&mut st, id, pool);
        if let Some(meta) = stored {
            Self::describe(e, meta);
            e.created_ns.get_or_insert(at_ns);
        }
        if e.key.is_none() {
            e.key = ev.key.clone();
        }
        e.priority = Some(ev.priority);
        if matches!(
            ev.kind,
            cronoq::EventKind::Updated | cronoq::EventKind::Replaced
        ) {
            // Not a transition: the status, attempt and timestamps stand.
            if e.status.is_none() {
                e.status = Some(ev.status);
            }
            Self::evict(&mut st, self.cap);
            return;
        }
        e.status = Some(ev.status);
        e.attempt = e.attempt.max(ev.attempt);
        if ev.status == TaskStatus::Running {
            e.started_ns = Some(at_ns);
        }
        if ev.status.is_terminal() {
            e.finished_ns = Some(at_ns);
        }
        // A successor's message names the task it follows; it is not an error of the task.
        if let Some(m) = &ev.message
            && ev.kind != cronoq::EventKind::Successor
        {
            e.message = Some(m.clone());
        }
        Self::evict(&mut st, self.cap);
    }

    /// Record the priority of a task that was just updated, if the task is known here.
    pub fn set_priority(&self, id: u64, priority: Priority) {
        if let Some(m) = self.inner.lock().unwrap().map.get_mut(&id) {
            m.priority = Some(priority);
        }
    }

    /// Details of a task.
    pub fn get(&self, id: u64) -> Option<TaskMeta> {
        self.inner.lock().unwrap().map.get(&id).cloned()
    }

    /// Finished tasks in id order: those in one of `pools` (any if empty) with one of
    /// `statuses` (any finished status if empty) and ids above `after`, at most `limit`.
    pub fn list_finished(
        &self,
        pools: &[&str],
        statuses: &[TaskStatus],
        after: u64,
        limit: usize,
    ) -> Vec<(u64, TaskMeta)> {
        let st = self.inner.lock().unwrap();
        let mut ids: Vec<u64> = st
            .map
            .iter()
            .filter(|(id, m)| {
                **id > after
                    && m.status.is_some_and(|s| {
                        s.is_terminal() && (statuses.is_empty() || statuses.contains(&s))
                    })
                    && (pools.is_empty() || pools.contains(&&*m.pool))
            })
            .map(|(id, _)| *id)
            .collect();
        ids.sort_unstable();
        ids.truncate(limit);
        ids.into_iter()
            .map(|id| (id, st.map[&id].clone()))
            .collect()
    }

    /// Latest task id seen for `key`.
    pub fn id_for_key(&self, key: &str) -> Option<u64> {
        self.inner.lock().unwrap().by_key.get(key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_evicts() {
        let c = TaskInfoCache::new(2);
        let p: Arc<str> = "p".into();
        c.on_submit(
            1,
            TaskMeta {
                pool: p.clone(),
                key: Some("k1".into()),
                max_attempts: 3,
                ..Default::default()
            },
        );
        let ev = |status, message: Option<&str>| StatusEvent {
            id: cronoq::TaskId(1),
            pool: cronoq::PoolId(0),
            key: None,
            status,
            attempt: 1,
            at: cronoq::Time(0),
            message: message.map(Arc::from),
            priority: cronoq::Priority::Normal,
            kind: cronoq::EventKind::Transition,
        };
        c.on_event(&ev(TaskStatus::Running, None), &p, 10);
        c.on_event(&ev(TaskStatus::Failed, Some("boom")), &p, 20);
        let m = c.get(1).unwrap();
        assert_eq!(
            (m.status, m.attempt, m.started_ns, m.finished_ns),
            (Some(TaskStatus::Failed), 1, Some(10), Some(20))
        );
        assert_eq!(m.message.as_deref(), Some("boom"));
        // An update is not a transition: only the priority follows it.
        let updated = StatusEvent {
            priority: cronoq::Priority::High,
            kind: cronoq::EventKind::Updated,
            attempt: 7,
            ..ev(TaskStatus::Running, None)
        };
        c.on_event(&updated, &p, 500);
        let m = c.get(1).unwrap();
        assert_eq!(
            (m.status, m.attempt, m.started_ns, m.finished_ns, m.priority),
            (
                Some(TaskStatus::Failed),
                1,
                Some(10),
                Some(20),
                Some(cronoq::Priority::High)
            )
        );
        assert_eq!(c.id_for_key("k1"), Some(1));
        c.on_submit(
            2,
            TaskMeta {
                pool: p.clone(),
                ..Default::default()
            },
        );
        c.on_submit(
            3,
            TaskMeta {
                pool: p.clone(),
                ..Default::default()
            },
        );
        assert!(
            c.get(1).is_none() && c.id_for_key("k1").is_none(),
            "oldest evicted"
        );
    }

    #[test]
    fn a_successor_takes_the_details_of_the_stored_submission() {
        let c = TaskInfoCache::new(10);
        let p: Arc<str> = "p".into();
        let ev = |id, status, kind, message: Option<&str>| StatusEvent {
            id: cronoq::TaskId(id),
            pool: cronoq::PoolId(0),
            key: Some("k".into()),
            status,
            attempt: 1,
            at: cronoq::Time(0),
            message: message.map(Arc::from),
            priority: cronoq::Priority::Normal,
            kind,
        };
        let meta = |kind: &str, max_attempts| TaskMeta {
            pool: p.clone(),
            key: Some("k".into()),
            kind: Some(kind.into()),
            max_attempts,
            ..Default::default()
        };
        use cronoq::EventKind::{Replaced, Successor, Transition};
        c.on_submit(1, meta("first", 3));
        c.on_event(&ev(1, TaskStatus::Running, Transition, None), &p, 10);
        c.on_pending(1, meta("second", 7));
        // Events of the running task leave the stored details alone.
        c.on_event(
            &ev(1, TaskStatus::Running, Replaced, Some("stored")),
            &p,
            11,
        );
        c.on_event(&ev(1, TaskStatus::Retrying, Transition, None), &p, 12);
        assert_eq!(c.get(1).unwrap().kind.as_deref(), Some("first"));
        c.on_event(&ev(1, TaskStatus::Succeeded, Transition, None), &p, 20);
        c.on_event(&ev(2, TaskStatus::Ready, Transition, None), &p, 20);
        c.on_event(
            &ev(2, TaskStatus::Ready, Successor, Some("follows task 1")),
            &p,
            20,
        );
        let m = c.get(2).unwrap();
        assert_eq!((m.kind.as_deref(), m.max_attempts), (Some("second"), 7));
        assert_eq!((m.status, m.message), (Some(TaskStatus::Ready), None));
        assert_eq!(c.id_for_key("k"), Some(2));
        assert_eq!(c.get(1).unwrap().max_attempts, 3);

        // Recorded after the task already ended: the successor is found under the key.
        c.on_event(&ev(2, TaskStatus::Running, Transition, None), &p, 30);
        c.on_event(&ev(2, TaskStatus::Succeeded, Transition, None), &p, 40);
        c.on_event(&ev(3, TaskStatus::Ready, Transition, None), &p, 40);
        c.on_event(
            &ev(3, TaskStatus::Ready, Successor, Some("follows task 2")),
            &p,
            40,
        );
        c.on_pending(2, meta("third", 9));
        assert_eq!(c.get(3).unwrap().max_attempts, 9);

        // A cancelled task takes the stored submission with it: a later task with the key
        // gets nothing of it.
        c.on_event(&ev(3, TaskStatus::Running, Transition, None), &p, 50);
        c.on_pending(3, meta("fourth", 11));
        c.on_event(&ev(3, TaskStatus::Cancelled, Transition, None), &p, 60);
        c.on_event(&ev(4, TaskStatus::Ready, Transition, None), &p, 70);
        assert_eq!(c.get(4).unwrap().max_attempts, 0);
    }

    #[test]
    fn lists_finished_tasks_by_pool_status_and_page() {
        let c = TaskInfoCache::new(10);
        let ev = |id, status| StatusEvent {
            id: cronoq::TaskId(id),
            pool: cronoq::PoolId(0),
            key: None,
            status,
            attempt: 1,
            at: cronoq::Time(0),
            message: None,
            priority: cronoq::Priority::Normal,
            kind: cronoq::EventKind::Transition,
        };
        let (a, b): (Arc<str>, Arc<str>) = ("a".into(), "b".into());
        c.on_event(&ev(4, TaskStatus::Succeeded), &a, 1);
        c.on_event(&ev(1, TaskStatus::Failed), &a, 1);
        c.on_event(&ev(2, TaskStatus::Running), &a, 1);
        c.on_event(&ev(3, TaskStatus::Succeeded), &b, 1);
        let ids = |v: Vec<(u64, TaskMeta)>| v.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
        assert_eq!(ids(c.list_finished(&[], &[], 0, 10)), [1, 3, 4]);
        assert_eq!(ids(c.list_finished(&["a"], &[], 0, 10)), [1, 4]);
        assert_eq!(
            ids(c.list_finished(&[], &[TaskStatus::Succeeded], 0, 10)),
            [3, 4]
        );
        assert_eq!(ids(c.list_finished(&[], &[], 1, 1)), [3]);
    }
}
