//! Bounded cache of task details for `Get` and for result messages.
//!
//! The scheduler engine only answers "what is the status of task X"; this cache adds the
//! pool, key, kind, attempt counts, timestamps and the last error message. It is filled when
//! tasks are submitted through this server and from the scheduler's event stream, and evicts
//! the oldest records beyond its capacity.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use cronoq::{StatusEvent, TaskStatus};
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
}

#[derive(Default)]
struct Inner {
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

    /// Record a status event of a task in `pool`, observed at `at_ns` (Unix ns).
    pub fn on_event(&self, ev: &StatusEvent, pool: &Arc<str>, at_ns: i64) {
        if self.cap == 0 {
            return;
        }
        let id = ev.id.0;
        let mut st = self.inner.lock().unwrap();
        if let Some(k) = &ev.key {
            st.by_key.entry(k.clone()).or_insert(id);
        }
        let e = Self::entry(&mut st, id, pool);
        if e.key.is_none() {
            e.key = ev.key.clone();
        }
        e.status = Some(ev.status);
        e.attempt = e.attempt.max(ev.attempt);
        if ev.status == TaskStatus::Running {
            e.started_ns = Some(at_ns);
        }
        if ev.status.is_terminal() {
            e.finished_ns = Some(at_ns);
        }
        if let Some(m) = &ev.message {
            e.message = Some(m.clone());
        }
        Self::evict(&mut st, self.cap);
    }

    /// Details of a task.
    pub fn get(&self, id: u64) -> Option<TaskMeta> {
        self.inner.lock().unwrap().map.get(&id).cloned()
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
        };
        c.on_event(&ev(TaskStatus::Running, None), &p, 10);
        c.on_event(&ev(TaskStatus::Failed, Some("boom")), &p, 20);
        let m = c.get(1).unwrap();
        assert_eq!(
            (m.status, m.attempt, m.started_ns, m.finished_ns),
            (Some(TaskStatus::Failed), 1, Some(10), Some(20))
        );
        assert_eq!(m.message.as_deref(), Some("boom"));
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
}
