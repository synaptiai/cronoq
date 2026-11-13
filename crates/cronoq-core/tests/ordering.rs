//! Ordering keys: tasks of one pool that share a key run one at a time, in submission order.

mod common;

use common::*;
use cronoq_core::*;

fn pool(workers: u32) -> Sim {
    Sim::with_pool(PoolConfig::fixed("default", workers))
}

fn job(d: f64) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d))
}

fn keyed(d: f64, key: &str) -> TaskSpec<Job> {
    job(d).ordering_key(key)
}

fn default_pool(s: &Sim) -> PoolId {
    s.core.pool_id("default").unwrap()
}

fn info(s: &Sim, key: &str) -> Option<OrderingInfo> {
    s.core.ordering(default_pool(s), key)
}

/// Start and end of every attempt, by task.
fn spans(s: &Sim) -> Vec<(TaskId, f64, f64)> {
    s.started
        .iter()
        .map(|x| {
            let at = x.at.diff_secs(Time::ZERO);
            (x.id, at, at + x.dur)
        })
        .collect()
}

#[test]
fn tasks_of_one_key_run_one_at_a_time_in_submission_order() {
    let mut s = pool(4);
    // Priorities do not reorder a key: order inside it is submission order.
    let classes = [
        Priority::Low,
        Priority::Critical,
        Priority::Normal,
        Priority::High,
        Priority::Low,
    ];
    let ids: Vec<TaskId> = classes
        .iter()
        .map(|c| s.submit(keyed(1.0, "account-1").priority(*c)))
        .collect();
    let free = s.submit(job(1.0));
    // The head and the task without a key start at once; the rest wait, with workers free.
    assert_eq!(s.order(), vec![ids[0], free]);
    assert_eq!(s.status(ids[1]), Some(TaskStatus::Ready));
    let snap = s.core.snapshot(s.now);
    assert_eq!(snap.pools[0].ordering.keys, 1);
    assert_eq!(snap.pools[0].ordering.waiting, 4);
    s.run_to_completion(20.0);
    let starts: Vec<f64> = ids
        .iter()
        .map(|id| s.start_of(*id).unwrap().diff_secs(Time::ZERO))
        .collect();
    assert_eq!(starts, vec![0.0, 1.0, 2.0, 3.0, 4.0]);
    // A key with no tasks left is forgotten.
    assert_eq!(info(&s, "account-1"), None);
}

#[test]
fn keys_do_not_wait_for_each_other() {
    let mut s = pool(8);
    let mut key_of = std::collections::HashMap::new();
    for round in 0..3 {
        for k in 0..1000 {
            let id = s.submit(keyed(1.0, &format!("k{k}")));
            key_of.insert(id, (k, round));
        }
    }
    s.run_to_completion(1000.0);
    assert_eq!(s.core.live_tasks(), 0);
    // All eight workers were busy throughout: 3,000 seconds of work in 375.
    let end = spans(&s).iter().map(|x| x.2).fold(0.0, f64::max);
    assert_eq!(end, 375.0);
    // Within a key: submission order, never two at once.
    let mut last: std::collections::HashMap<i32, (i32, f64)> = Default::default();
    for (id, start, stop) in spans(&s) {
        let (k, round) = key_of[&id];
        if let Some((prev_round, prev_stop)) = last.insert(k, (round, stop)) {
            assert!(prev_round < round && prev_stop <= start, "key k{k}");
        }
    }
}

#[test]
fn a_key_belongs_to_its_pool() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("a", 2), PoolConfig::fixed("b", 2)],
        ..Default::default()
    });
    let a = s.submit(keyed(5.0, "k").pool("a"));
    let b = s.submit(keyed(5.0, "k").pool("b"));
    assert_eq!(s.order(), vec![a, b]);
}

#[test]
fn an_ordering_key_is_not_a_deduplication_key() {
    let mut s = pool(2);
    // Two submissions with the same ordering key are two tasks.
    let a = s.submit(keyed(1.0, "k"));
    let b = s.submit(keyed(1.0, "k"));
    assert_ne!(a, b);
    // A task may have both: the dedup key still answers duplicates.
    let c = s.submit(keyed(1.0, "k").key("once"));
    let again = s.try_submit(keyed(1.0, "k").key("once")).unwrap();
    assert_eq!((again.id, again.duplicate), (c, true));
    assert_eq!(info(&s, "k").unwrap().tasks, 3);
}

#[test]
fn the_head_runs_at_the_highest_class_waiting_in_its_key() {
    let mut s = pool(1);
    let busy = s.submit(job(1.0));
    let head = s.submit(keyed(1.0, "k").priority(Priority::Low));
    let normal = s.submit(job(1.0));
    assert_eq!(info(&s, "k").unwrap().effective, Some(Priority::Low));
    // A high task joins behind the low head: the head is lifted, so the high task is not
    // stuck behind work of the normal class.
    let high = s.submit(keyed(1.0, "k").priority(Priority::High));
    let i = info(&s, "k").unwrap();
    assert_eq!(
        (i.head, i.effective, i.tasks),
        (Some(head), Some(Priority::High), 2)
    );
    // The head's own class is unchanged.
    assert_eq!(
        s.core.task(&TaskRef::Id(head)).unwrap().priority,
        Priority::Low
    );
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![busy, head, high, normal]);
}

#[test]
fn the_lift_follows_the_tasks_that_wait() {
    let mut s = pool(1);
    let busy = s.submit(job(1.0));
    let head = s.submit(keyed(1.0, "k").priority(Priority::Low));
    let normal = s.submit(job(1.0));
    let high = s.submit(keyed(1.0, "k").priority(Priority::High));
    assert_eq!(info(&s, "k").unwrap().effective, Some(Priority::High));
    // The high task is cancelled: the head goes back to its own class.
    assert!(s.core.cancel(s.now, &TaskRef::Id(high)));
    s.drain();
    assert_eq!(info(&s, "k").unwrap().effective, Some(Priority::Low));
    // An update of a waiting task lifts the head too, and lowering it undoes that.
    let tail = s.submit(keyed(1.0, "k").priority(Priority::Low));
    s.update(tail, TaskPatch::default().priority(Priority::Critical))
        .unwrap();
    assert_eq!(info(&s, "k").unwrap().effective, Some(Priority::Critical));
    s.update(tail, TaskPatch::default().priority(Priority::Low))
        .unwrap();
    assert_eq!(info(&s, "k").unwrap().effective, Some(Priority::Low));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![busy, normal, head, tail]);
}

#[test]
fn a_floor_lifts_the_head_and_is_cleared_with_the_key() {
    let mut s = pool(1);
    let p = default_pool(&s);
    let busy = s.submit(job(1.0));
    let normal = s.submit(job(1.0));
    let low = s.submit(keyed(1.0, "k").priority(Priority::Low));
    s.core
        .set_ordering_floor(s.now, p, "k", Some(Priority::High), false)
        .unwrap();
    s.drain();
    let i = info(&s, "k").unwrap();
    assert_eq!(
        (i.floor, i.effective),
        (Some(Priority::High), Some(Priority::High))
    );
    // A floor never lowers a key.
    let critical = s.submit(keyed(1.0, "k").priority(Priority::Critical));
    assert_eq!(info(&s, "k").unwrap().effective, Some(Priority::Critical));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![busy, low, critical, normal]);
    // The key emptied: the floor went with it.
    assert_eq!(info(&s, "k"), None);
    let busy = s.submit(job(1.0));
    let normal = s.submit(job(1.0));
    let low = s.submit(keyed(1.0, "k").priority(Priority::Low));
    s.run_to_completion(20.0);
    assert_eq!(s.order()[4..], [busy, normal, low]);
}

#[test]
fn a_kept_floor_outlives_its_tasks_and_is_limited_in_number() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        max_ordering_settings: 1,
        persist: true,
        ..Default::default()
    });
    let p = default_pool(&s);
    // Set before the key has any task.
    s.core
        .set_ordering_floor(s.now, p, "vip", Some(Priority::High), true)
        .unwrap();
    s.drain();
    assert!(matches!(
        s.persisted.last(),
        Some(Mutation::Ordering { key, floor: Some(Priority::High), persist: true, halted: false, .. })
            if &**key == "vip"
    ));
    for _ in 0..2 {
        let busy = s.submit(job(1.0));
        let normal = s.submit(job(1.0));
        let low = s.submit(keyed(1.0, "vip").priority(Priority::Low));
        s.run_to_completion(20.0);
        let n = s.order().len();
        assert_eq!(s.order()[n - 3..], [busy, low, normal]);
        assert_eq!(info(&s, "vip").unwrap().tasks, 0);
    }
    assert_eq!(s.core.snapshot(s.now).pools[0].ordering.settings, 1);
    // The limit refuses a second kept floor, never a floor that goes with its tasks, and
    // never a submission.
    assert_eq!(
        s.core
            .set_ordering_floor(s.now, p, "other", Some(Priority::High), true),
        Err(OrderingError::TooManySettings)
    );
    assert_eq!(info(&s, "other"), None);
    let t1 = s.submit(keyed(5.0, "other"));
    s.core
        .set_ordering_floor(s.now, p, "other", Some(Priority::High), false)
        .unwrap();
    // Changing the kept one is always possible; removing it frees the place.
    s.core
        .set_ordering_floor(s.now, p, "vip", Some(Priority::Critical), true)
        .unwrap();
    s.core
        .set_ordering_floor(s.now, p, "vip", None, false)
        .unwrap();
    s.drain();
    assert_eq!(info(&s, "vip"), None);
    assert!(matches!(
        s.persisted.last(),
        Some(Mutation::Ordering { key, floor: None, persist: false, .. }) if &**key == "vip"
    ));
    s.core
        .set_ordering_floor(s.now, p, "other", Some(Priority::High), true)
        .unwrap();
    s.drain();
    assert_eq!(s.status(t1), Some(TaskStatus::Running));
    assert_eq!(
        s.core
            .set_ordering_floor(s.now, PoolId(9), "k", Some(Priority::High), false),
        Err(OrderingError::UnknownPool)
    );
}

#[test]
fn a_head_that_retries_keeps_its_place() {
    let mut s = pool(2);
    let retry = RetryPolicy {
        max_attempts: 3,
        initial_backoff: secs(10.0),
        multiplier: 1.0,
        max_backoff: secs(10.0),
        jitter: false,
    };
    let head = s.submit(
        TaskSpec::new(Job::flaky(1.0, 2))
            .ordering_key("k")
            .retry(retry),
    );
    let next = s.submit(keyed(1.0, "k"));
    s.run_until(t(5.0));
    // The head waits for its retry; the next task does not overtake it.
    assert_eq!(s.status(head), Some(TaskStatus::Retrying));
    assert_eq!(s.status(next), Some(TaskStatus::Ready));
    assert_eq!(s.order(), vec![head]);
    s.run_to_completion(60.0);
    assert_eq!(s.order(), vec![head, head, head, next]);
    assert_eq!(s.status(next), Some(TaskStatus::Succeeded));
}

#[test]
fn a_failed_task_does_not_stop_its_key_unless_asked() {
    let mut s = pool(2);
    let bad = s.submit(TaskSpec::new(Job::broken(1.0)).ordering_key("k"));
    let next = s.submit(keyed(1.0, "k"));
    s.run_to_completion(20.0);
    assert_eq!(s.status(bad), Some(TaskStatus::Failed));
    assert_eq!(s.status(next), Some(TaskStatus::Succeeded));
}

#[test]
fn halt_on_failure_stops_the_key_until_it_is_resumed() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 2)],
        persist: true,
        ..Default::default()
    });
    let p = default_pool(&s);
    let bad = s.submit(
        TaskSpec::new(Job::broken(1.0))
            .ordering_key("k")
            .halt_on_failure(),
    );
    let next = s.submit(keyed(1.0, "k"));
    let last = s.submit(keyed(1.0, "k"));
    let other = s.submit(keyed(1.0, "other"));
    s.run_until(t(10.0));
    assert_eq!(s.status(bad), Some(TaskStatus::Failed));
    assert_eq!(s.status(other), Some(TaskStatus::Succeeded));
    // The rest of the key waits, in place.
    assert_eq!(s.status(next), Some(TaskStatus::Ready));
    assert_eq!(s.start_of(next), None);
    let i = info(&s, "k").unwrap();
    assert!(i.halted);
    assert_eq!((i.head, i.tasks), (Some(next), 2));
    assert_eq!(s.core.halted_orderings(p, 10), vec!["k".into()]);
    let snap = s.core.snapshot(s.now);
    assert_eq!(
        (
            snap.pools[0].ordering.halted,
            snap.pools[0].ordering.waiting
        ),
        (1, 2)
    );
    assert!(
        s.persisted
            .iter()
            .any(|m| matches!(m, Mutation::Ordering { key, halted: true, .. } if &**key == "k"))
    );
    // Submissions are still accepted and wait behind.
    let late = s.submit(keyed(1.0, "k"));
    assert_eq!(s.start_of(late), None);
    // Resuming is idempotent.
    assert!(s.core.resume_ordering(s.now, p, "k"));
    assert!(!s.core.resume_ordering(s.now, p, "k"));
    assert!(!s.core.resume_ordering(s.now, p, "unknown"));
    s.drain();
    assert!(matches!(
        s.persisted
            .iter()
            .rev()
            .find(|m| matches!(m, Mutation::Ordering { .. })),
        Some(Mutation::Ordering { halted: false, .. })
    ));
    s.run_to_completion(20.0);
    let n = s.order().len();
    assert_eq!(s.order()[n - 3..], [next, last, late]);
}

#[test]
fn only_a_failure_halts_and_cancelling_the_rest_ends_the_halt() {
    let mut s = pool(1);
    let busy = s.submit(job(5.0));
    // A cancelled task with the halt policy does not halt.
    let a = s.submit(keyed(1.0, "k").halt_on_failure());
    let b = s.submit(keyed(1.0, "k"));
    assert!(s.core.cancel(s.now, &TaskRef::Id(a)));
    s.drain();
    assert!(!info(&s, "k").unwrap().halted);
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![busy, b]);
    // A dead-lettered one does (retries exhausted).
    let retry = RetryPolicy {
        max_attempts: 2,
        initial_backoff: secs(1.0),
        multiplier: 1.0,
        max_backoff: secs(1.0),
        jitter: false,
    };
    let bad = s.submit(
        TaskSpec::new(Job::flaky(1.0, 5))
            .ordering_key("k")
            .retry(retry)
            .halt_on_failure(),
    );
    let c = s.submit(keyed(1.0, "k"));
    s.run_until(s.now + secs(20.0));
    assert!(s.status(bad).unwrap().is_terminal());
    assert_ne!(s.status(bad), Some(TaskStatus::Succeeded));
    assert!(info(&s, "k").unwrap().halted);
    // Cancelling what waits empties the key, and the halt goes with it.
    assert!(s.core.cancel(s.now, &TaskRef::Id(c)));
    s.drain();
    assert_eq!(info(&s, "k"), None);
    let d = s.submit(keyed(1.0, "k"));
    s.run_to_completion(20.0);
    assert_eq!(s.status(d), Some(TaskStatus::Succeeded));
}

#[test]
fn a_cancelled_task_leaves_its_key() {
    let mut s = pool(1);
    let busy = s.submit(job(1.0));
    let a = s.submit(keyed(1.0, "k"));
    let b = s.submit(keyed(1.0, "k"));
    let c = s.submit(keyed(1.0, "k"));
    // One that waits behind, then the head itself.
    assert!(s.core.cancel(s.now, &TaskRef::Id(b)));
    s.drain();
    assert_eq!(info(&s, "k").unwrap().tasks, 2);
    assert!(s.core.cancel(s.now, &TaskRef::Id(a)));
    s.drain();
    assert_eq!(info(&s, "k").unwrap().head, Some(c));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![busy, c]);
}

#[test]
fn a_task_blocked_on_a_dependency_holds_those_behind_it() {
    let mut s = pool(4);
    let parent = s.submit(job(5.0));
    let blocked = s.submit(keyed(1.0, "k").after(parent));
    let behind = s.submit(keyed(1.0, "k"));
    s.run_until(t(3.0));
    assert_eq!(s.status(blocked), Some(TaskStatus::Blocked));
    assert_eq!(s.status(behind), Some(TaskStatus::Ready));
    assert_eq!(s.start_of(behind), None);
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![parent, blocked, behind]);
    // A dependency between two tasks of one key is fine in submission order.
    let first = s.submit(keyed(1.0, "j"));
    let second = s.submit(keyed(1.0, "j").after(first));
    s.run_to_completion(20.0);
    assert_eq!(s.order()[3..], [first, second]);
}

#[test]
fn fixed_starts_and_recurring_tasks_cannot_have_an_ordering_key() {
    let mut s = pool(1);
    let fixed = keyed(1.0, "k").fixed_start(FixedStart::at(t(100.0)));
    assert!(matches!(s.try_submit(fixed), Err(SubmitError::Invalid(_))));
    let recurring = RecurringSpec {
        name: "nightly".into(),
        cron: "0 * * * * *".into(),
        tz: None,
        template: keyed(1.0, "k"),
        deadline_after: None,
        overlap: OverlapPolicy::Skip,
        tolerance: secs(1.0),
        on_miss: MissPolicy::RunLate,
    };
    assert!(s.core.add_recurring(s.now, recurring).is_err());
    assert_eq!(info(&s, "k"), None);
}

#[test]
fn a_replacement_keeps_the_place_in_the_key() {
    let mut s = pool(1);
    let running = s.submit(keyed(5.0, "k").key("a"));
    let b = s.submit(keyed(1.0, "k"));
    // The ordering key is not something a replacement changes.
    assert!(matches!(
        s.replace(keyed(1.0, "other").key("a"), false),
        Err(SubmitError::ReplaceMismatch(_))
    ));
    assert!(matches!(
        s.replace(job(1.0).key("a"), false),
        Err(SubmitError::ReplaceMismatch(_))
    ));
    // Stored behind the running task, the successor takes its place: ahead of `b`.
    assert_eq!(
        s.replace(keyed(2.0, "k").key("a"), false).unwrap().outcome,
        SubmitOutcome::Pending
    );
    s.run_to_completion(30.0);
    let order = s.order();
    assert_eq!((order.len(), order[0], order[2]), (3, running, b));
    assert_eq!(s.started[1].dur, 2.0);
    // A queued task replaced with a renewed position goes to the back of its key.
    let busy = s.submit(job(1.0));
    let x = s.submit(keyed(1.0, "q").key("x"));
    let y = s.submit(keyed(1.0, "q"));
    assert_eq!(info(&s, "q").unwrap().head, Some(x));
    s.replace(keyed(1.0, "q").key("x"), true).unwrap();
    assert_eq!(info(&s, "q").unwrap().head, Some(y));
    s.run_to_completion(30.0);
    assert_eq!(s.order()[3..], [busy, y, x]);
}

#[test]
fn bounds_count_every_task_and_only_a_head_is_shed() {
    let mut s = Sim::with_pool(PoolConfig {
        bounds: Bounds {
            max_pending: Some(3),
            ..Default::default()
        },
        overflow: OverflowPolicy::ShedLowest,
        ..PoolConfig::fixed("default", 1)
    });
    let busy = s.submit(job(10.0));
    let head = s.submit(keyed(1.0, "k").priority(Priority::Low));
    let behind = s.submit(keyed(1.0, "k").priority(Priority::Low));
    let lone = s.submit(job(1.0).priority(Priority::Low));
    // Full. A normal task sheds the newest low task that is offered: not `behind`, which
    // is newer than the head but waits behind it.
    let n1 = s.submit(job(1.0));
    assert_eq!(s.status(lone), Some(TaskStatus::Shed));
    assert_eq!(s.status(behind), Some(TaskStatus::Ready));
    // Next the head goes, and the task behind it becomes the head.
    let n2 = s.submit(job(1.0));
    assert_eq!(s.status(head), Some(TaskStatus::Shed));
    assert_eq!(info(&s, "k").unwrap().head, Some(behind));
    // A head lifted by a high task behind it is still shed as the low task it is.
    let high = s
        .try_submit(keyed(1.0, "k").priority(Priority::High))
        .unwrap()
        .id;
    assert_eq!(s.status(behind), Some(TaskStatus::Shed));
    assert_eq!(info(&s, "k").unwrap().head, Some(high));
    s.run_to_completion(40.0);
    assert_eq!(s.order(), vec![busy, high, n1, n2]);
}

fn rec_task(id: u64, seq: u64, spec: TaskSpec<Job>) -> RecoveredTask<Job> {
    RecoveredTask {
        id: TaskId(id),
        seq,
        arrival: Time::ZERO,
        spec,
        attempt: 0,
    }
}

#[test]
fn keys_are_rebuilt_from_the_stored_order_after_recovery() {
    let cfg = || CoreConfig {
        pools: vec![PoolConfig::fixed("default", 4)],
        persist: true,
        ..Default::default()
    };
    // What a store holds: the ordering key and failure policy travel with the task.
    let mut s = Sim::new(cfg());
    s.submit(keyed(1.0, "k").halt_on_failure());
    let Some(Mutation::Submitted { spec, .. }) = s.persisted.first() else {
        panic!("persisted");
    };
    assert_eq!(spec.ordering_key.as_deref(), Some("k"));
    assert_eq!(spec.on_failure, OrderFailure::Halt);

    let mut r = Sim::new(cfg());
    r.core.recover(
        r.now,
        Recovered {
            // Not in order, and one that had already started before the failover.
            tasks: vec![
                rec_task(3, 30, keyed(1.0, "k")),
                RecoveredTask {
                    attempt: 1,
                    ..rec_task(1, 10, keyed(1.0, "k"))
                },
                rec_task(2, 20, keyed(1.0, "k")),
                rec_task(5, 50, keyed(1.0, "stopped")),
                rec_task(4, 40, keyed(1.0, "stopped").priority(Priority::Low)),
            ],
            ordering: vec![
                RecoveredOrdering {
                    pool: "default".into(),
                    key: "stopped".into(),
                    floor: Some(Priority::High),
                    persist: true,
                    halted: true,
                },
                RecoveredOrdering {
                    pool: "gone".into(),
                    key: "x".into(),
                    floor: None,
                    persist: false,
                    halted: true,
                },
            ],
            next_id: 6,
            ..Default::default()
        },
    );
    r.drain();
    // The oldest task of `k` is its head (its lost attempt is retried after a backoff); the
    // others wait for it, and the halted key offers nothing.
    assert_eq!(info(&r, "k").unwrap().head, Some(TaskId(1)));
    r.run_until(t(0.5));
    assert!(r.order().iter().all(|id| *id == TaskId(1)));
    let i = info(&r, "stopped").unwrap();
    assert!(i.halted && i.persist);
    assert_eq!((i.head, i.floor), (Some(TaskId(4)), Some(Priority::High)));
    let p = default_pool(&r);
    assert!(r.core.resume_ordering(r.now, p, "stopped"));
    r.run_to_completion(120.0);
    assert_eq!(r.core.live_tasks(), 0);
    let order = r.order();
    let at = |id: u64| order.iter().position(|x| *x == TaskId(id)).unwrap();
    assert!(at(1) < at(2) && at(2) < at(3), "{order:?}");
    assert!(at(4) < at(5), "{order:?}");
    // The kept floor is still there.
    assert_eq!(info(&r, "stopped").unwrap().floor, Some(Priority::High));
}

#[test]
fn ordering_keys_work_in_a_pool_with_fair_sharing() {
    let mut s = Sim::with_pool(PoolConfig {
        fairness: Some(Fairness::weights([("a", 1.0), ("b", 1.0)])),
        ..PoolConfig::fixed("default", 1)
    });
    let busy = s.submit(job(1.0).tenant("a"));
    let head = s.submit(keyed(1.0, "k").tenant("a").priority(Priority::Low));
    let normal = s.submit(job(1.0).tenant("b"));
    let high = s.submit(keyed(1.0, "k").tenant("a").priority(Priority::High));
    let tail = s.submit(keyed(1.0, "k").tenant("b"));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![busy, head, high, normal, tail]);
}

fn awaiting() -> Sim {
    let mut p = PoolConfig::fixed("default", 2);
    p.unknown_dependency = UnknownDependency::Await { timeout: None };
    Sim::with_pool(p)
}

#[test]
fn a_task_cannot_wait_for_one_behind_it_in_its_key() {
    // Directly: the first task of the key waits for a key that the second would define.
    let mut s = awaiting();
    let first = s.submit(keyed(1.0, "k").after("later"));
    assert_eq!(
        s.try_submit(keyed(1.0, "k").key("later")),
        Err(SubmitError::Cycle)
    );
    // The same task outside the ordering key is fine.
    let later = s.submit(job(1.0).key("later"));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![later, first]);

    // Through a task without the ordering key, and through another key.
    let mut s = awaiting();
    let between = s.submit(job(1.0).after("later").key("between"));
    let other = s.submit(keyed(1.0, "other").after("between"));
    let behind_other = s.submit(keyed(1.0, "other").key("behind"));
    let head = s.submit(keyed(1.0, "k").after("behind"));
    assert_eq!(
        s.try_submit(keyed(1.0, "k").key("later")),
        Err(SubmitError::Cycle)
    );
    let later = s.submit(job(1.0).key("later"));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![later, between, other, behind_other, head]);
}

#[test]
fn a_batch_keeps_its_order_within_a_key_and_refuses_a_cycle() {
    let mut s = awaiting();
    let busy = s.submit(keyed(1.0, "k"));
    // The second task of the batch depends on the fourth; within the key the batch order
    // holds all the same.
    let batch = vec![
        keyed(1.0, "k"),
        job(1.0).after("d"),
        keyed(1.0, "k"),
        job(1.0).key("d"),
        keyed(1.0, "k"),
    ];
    let ids: Vec<TaskId> = s
        .core
        .submit_batch(s.now, batch)
        .unwrap()
        .iter()
        .map(|x| x.id)
        .collect();
    s.drain();
    s.run_to_completion(20.0);
    let order = s.order();
    let at = |id: TaskId| order.iter().position(|x| *x == id).unwrap();
    assert!(at(busy) < at(ids[0]) && at(ids[0]) < at(ids[2]) && at(ids[2]) < at(ids[4]));
    assert!(at(ids[3]) < at(ids[1]));
    // A dependency against the order of the key is a cycle: inside the batch,
    let now = s.now;
    let inside = vec![keyed(1.0, "k").after("x"), keyed(1.0, "k").key("x")];
    assert!(matches!(
        s.core.submit_batch(now, inside),
        Err((_, SubmitError::Cycle))
    ));
    // and through a task that is already there.
    let head = s.submit(keyed(1.0, "k").after("y"));
    let through = vec![job(1.0), keyed(1.0, "k").key("y")];
    assert!(matches!(
        s.core.submit_batch(now, through),
        Err((_, SubmitError::Cycle))
    ));
    assert_eq!(s.core.live_tasks(), 1);
    assert!(s.core.cancel(now, &TaskRef::Id(head)));
    s.drain();
}

#[test]
fn a_renewed_place_cannot_be_behind_a_task_that_waits_for_it() {
    let mut s = pool(1);
    let busy = s.submit(job(5.0));
    let first = s.submit(keyed(1.0, "k").key("first"));
    let second = s.submit(keyed(1.0, "k").after(first));
    // Going to the back of the key would put `first` behind a task that waits for it.
    assert_eq!(
        s.replace(keyed(2.0, "k").key("first"), true),
        Err(SubmitError::Cycle)
    );
    // Keeping the place is fine.
    assert_eq!(
        s.replace(keyed(2.0, "k").key("first"), false)
            .unwrap()
            .outcome,
        SubmitOutcome::Replaced
    );
    s.run_to_completion(30.0);
    assert_eq!(s.order(), vec![busy, first, second]);
}
