//! `replace_queued`: the newest submission of a key wins. A task that has not started is
//! replaced in place; a task that has started keeps its payload and is followed by a successor.

mod common;

use common::*;
use cronoq_core::*;

fn one_worker() -> Sim {
    Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    })
}

fn job(d: f64) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d))
}

fn keyed(d: f64) -> TaskSpec<Job> {
    job(d).key("k")
}

/// The payload (`dur`) each attempt of `id` was handed.
fn payloads(s: &Sim, id: TaskId) -> Vec<f64> {
    s.started
        .iter()
        .filter(|x| x.id == id)
        .map(|x| x.dur)
        .collect()
}

// ------------------------------------------------------------------------------------------
// A task that has not started is replaced in place
// ------------------------------------------------------------------------------------------

#[test]
fn ten_submissions_of_one_key_run_once_with_the_last_payload() {
    let mut s = one_worker();
    s.submit(job(10.0));
    let first = s.submit(keyed(1.0));
    for i in 2..=10 {
        let r = s.replace(keyed(i as f64), false).unwrap();
        assert_eq!(
            (r.id, r.duplicate, r.outcome),
            (first, true, SubmitOutcome::Replaced)
        );
    }
    s.run_to_completion(100.0);
    assert_eq!(payloads(&s, first), [10.0]);
    assert_eq!(s.started.len(), 2, "the blocker and one run of the key");
    assert_eq!(s.core.totals().superseded, 9);
    assert_eq!(s.core.totals().submitted, 2);
}

#[test]
fn a_replaced_task_keeps_its_place_unless_the_position_is_renewed() {
    for renew in [false, true] {
        let mut s = one_worker();
        s.submit(job(10.0));
        let a = s.submit(keyed(1.0));
        let b = s.submit(job(1.0));
        assert_eq!(s.replace(keyed(2.0), renew).unwrap().id, a);
        let c = s.submit(job(1.0));
        s.run_to_completion(100.0);
        let order = s.order()[1..].to_vec();
        if renew {
            assert_eq!(order, [b, a, c], "takes the place of the new submission");
        } else {
            assert_eq!(order, [a, b, c], "keeps the place of the first submission");
        }
    }
}

#[test]
fn a_replacement_brings_its_priority_deadline_tags_and_kind() {
    let mut s = one_worker();
    s.submit(job(10.0));
    let normal = s.submit(job(1.0));
    let id = s.submit(keyed(1.0).priority(Priority::Low).tag("v", "1"));
    let dependent = s.submit(job(1.0).after(id));
    let r = s
        .replace(
            keyed(2.0)
                .priority(Priority::High)
                .kind("other")
                .tag("v", "2")
                .tag("cronoq.content_type", "text/plain"),
            false,
        )
        .unwrap();
    assert_eq!(r.outcome, SubmitOutcome::Replaced);
    s.run_to_completion(100.0);
    assert_eq!(s.order()[1..], [id, normal, dependent]);
    let run = s.started.iter().find(|x| x.id == id).unwrap();
    assert_eq!((run.dur, run.kind.as_str()), (2.0, "other"));
    assert_eq!(
        run.tags,
        [
            ("v".to_string(), "2".to_string()),
            ("cronoq.content_type".to_string(), "text/plain".to_string())
        ]
    );
    assert_eq!(s.status(dependent), Some(TaskStatus::Succeeded));
}

#[test]
fn a_blocked_task_is_replaced_in_place_and_still_waits_for_its_dependency() {
    let mut s = one_worker();
    let parent = s.submit(job(5.0).key("parent"));
    let child = s.submit(keyed(1.0).after("parent"));
    let r = s.replace(keyed(3.0).after("parent"), false).unwrap();
    assert_eq!((r.id, r.outcome), (child, SubmitOutcome::Replaced));
    assert_eq!(s.status(child), Some(TaskStatus::Blocked));
    s.run_to_completion(100.0);
    assert_eq!(s.order(), [parent, child]);
    assert_eq!(payloads(&s, child), [3.0]);
}

#[test]
fn without_a_live_task_a_replacing_submission_creates_one() {
    let mut s = one_worker();
    let r = s.replace(keyed(1.0), false).unwrap();
    assert_eq!((r.duplicate, r.outcome), (false, SubmitOutcome::Created));
    s.run_to_completion(100.0);
    assert_eq!(s.status(r.id), Some(TaskStatus::Succeeded));
    // The key is retained by the finished task: a plain submission returns it, a replacing
    // one starts a new task.
    let plain = s.try_submit(keyed(2.0)).unwrap();
    assert_eq!((plain.id, plain.outcome), (r.id, SubmitOutcome::Existing));
    let again = s.replace(keyed(2.0), false).unwrap();
    assert_eq!(again.outcome, SubmitOutcome::Created);
    assert_ne!(again.id, r.id);
    s.run_to_completion(100.0);
    assert_eq!(payloads(&s, again.id), [2.0]);
    let latest = s.try_submit(keyed(3.0)).unwrap();
    assert_eq!(latest.id, again.id, "the key names the newest task");
}

// ------------------------------------------------------------------------------------------
// A task that has started keeps its payload; the newest submission runs after it
// ------------------------------------------------------------------------------------------

#[test]
fn a_running_task_keeps_its_payload_and_the_newest_submission_follows_it() {
    let mut s = one_worker();
    let first = s.submit(keyed(10.0));
    let between = s.submit(job(1.0));
    for d in [2.0, 3.0, 4.0] {
        let r = s.replace(keyed(d), false).unwrap();
        assert_eq!(
            (r.id, r.duplicate, r.outcome),
            (first, true, SubmitOutcome::Pending)
        );
    }
    assert_eq!(
        s.core.totals().superseded,
        2,
        "two stored submissions were dropped"
    );
    s.run_to_completion(100.0);
    let successor = s.try_submit(keyed(9.0)).unwrap().id;
    assert_ne!(successor, first);
    assert_eq!(payloads(&s, first), [10.0]);
    assert_eq!(payloads(&s, successor), [4.0]);
    // It took the place the first task held, ahead of the task submitted after that one.
    assert_eq!(s.order(), [first, successor, between]);
    assert_eq!(s.core.totals().submitted, 3);
}

#[test]
fn a_task_waiting_for_a_retry_keeps_its_payload_for_every_attempt() {
    let mut s = one_worker();
    let flaky = s.submit(
        TaskSpec::new(Job::flaky(1.0, 2))
            .key("k")
            .retry(RetryPolicy {
                max_attempts: 3,
                initial_backoff: secs(5.0),
                jitter: false,
                ..Default::default()
            }),
    );
    s.run_until(t(2.0));
    assert_eq!(s.status(flaky), Some(TaskStatus::Retrying));
    let r = s.replace(keyed(7.0), false).unwrap();
    assert_eq!((r.id, r.outcome), (flaky, SubmitOutcome::Pending));
    s.run_to_completion(200.0);
    assert_eq!(payloads(&s, flaky), [1.0, 1.0, 1.0]);
    assert_eq!(s.status(flaky), Some(TaskStatus::Succeeded));
    let successor = s.try_submit(keyed(9.0)).unwrap().id;
    assert_eq!(payloads(&s, successor), [7.0]);
}

#[test]
fn a_successor_follows_a_failure_but_not_a_cancel() {
    // Failed for good: the stored submission still runs.
    let mut s = one_worker();
    let broken = s.submit(TaskSpec::new(Job::broken(1.0)).key("k"));
    s.replace(keyed(2.0), false).unwrap();
    s.run_to_completion(100.0);
    assert_eq!(s.status(broken), Some(TaskStatus::Failed));
    let successor = s.try_submit(keyed(9.0)).unwrap().id;
    assert_ne!(successor, broken);
    assert_eq!(s.status(successor), Some(TaskStatus::Succeeded));

    // Cancelled: the stored submission is dropped with the task.
    let mut s = one_worker();
    let running = s.submit(keyed(10.0));
    s.replace(keyed(2.0), false).unwrap();
    let now = s.now;
    assert!(s.core.cancel(now, &TaskRef::Id(running)));
    s.drain();
    s.run_to_completion(100.0);
    assert_eq!(s.started.len(), 1);
    assert_eq!(s.core.live_tasks(), 0);
    let same = s.try_submit(keyed(9.0)).unwrap();
    assert_eq!((same.id, same.outcome), (running, SubmitOutcome::Existing));
}

#[test]
fn renewing_the_position_moves_the_successor_behind_tasks_submitted_in_between() {
    for renew in [false, true] {
        let mut s = one_worker();
        let first = s.submit(keyed(10.0));
        let b = s.submit(job(1.0));
        // Only the last replacing submission decides.
        s.replace(keyed(2.0), !renew).unwrap();
        let c = s.submit(job(1.0));
        s.replace(keyed(3.0), renew).unwrap();
        let d = s.submit(job(1.0));
        s.run_to_completion(100.0);
        let successor = s.try_submit(keyed(9.0)).unwrap().id;
        if renew {
            assert_eq!(s.order(), [first, b, c, successor, d]);
        } else {
            assert_eq!(s.order(), [first, successor, b, c, d]);
        }
        assert_eq!(payloads(&s, successor), [3.0]);
    }
}

#[test]
fn a_successor_does_not_wait_for_the_dependencies_again() {
    let mut s = one_worker();
    let parent = s.submit(job(1.0).key("parent"));
    let child = s.submit(keyed(10.0).after("parent"));
    s.run_until(t(2.0));
    assert_eq!(s.status(child), Some(TaskStatus::Running));
    assert_eq!(
        s.replace(keyed(3.0).after("parent"), false)
            .unwrap()
            .outcome,
        SubmitOutcome::Pending
    );
    s.run_to_completion(100.0);
    let successor = s.try_submit(keyed(9.0)).unwrap().id;
    assert_eq!(s.order(), [parent, child, successor]);
}

#[test]
fn a_key_with_dependencies_can_be_replaced_again_after_its_successor() {
    let cfg = || CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    };
    let mut s = Sim::new(cfg());
    s.submit(job(1.0).key("parent"));
    let child = s.submit(keyed(10.0).after("parent"));
    s.run_until(t(2.0));
    s.replace(keyed(20.0).after("parent"), false).unwrap();
    s.run_until(t(12.0));
    // The successor is running. The same producer sends the next value, described the same
    // way: stored behind the successor. Then one more task with the key, replaced in place.
    let successor = s.try_submit(keyed(9.0)).unwrap().id;
    assert_ne!(successor, child);
    assert_eq!(s.status(successor), Some(TaskStatus::Running));
    let r = s.replace(keyed(3.0).after("parent"), false).unwrap();
    assert_eq!((r.id, r.outcome), (successor, SubmitOutcome::Pending));
    // A replacement that names other dependencies is still refused.
    assert_eq!(
        s.replace(keyed(3.0), false),
        Err(SubmitError::ReplaceMismatch("the dependencies differ"))
    );
    s.run_until(t(33.0));
    let third = s.try_submit(keyed(9.0)).unwrap().id;
    assert!(third != successor && s.status(third) == Some(TaskStatus::Running));
    s.submit(job(50.0));
    let waiting = s.replace(keyed(4.0).after("parent"), false).unwrap();
    assert_eq!(waiting.outcome, SubmitOutcome::Pending);

    // After a restart the successor is still a successor: it does not wait for the
    // dependencies again, and takes a replacement that names them.
    let mut tasks: Vec<RecoveredTask<Job>> = Vec::new();
    for m in &s.persisted {
        match m {
            Mutation::Submitted {
                id,
                seq,
                arrival,
                spec,
            } => tasks.push(RecoveredTask {
                id: *id,
                seq: *seq,
                arrival: *arrival,
                spec: spec.clone(),
                attempt: 0,
                releases: 0,
            }),
            Mutation::Status { id, status, .. } if status.is_terminal() => {
                tasks.retain(|t| t.id != *id)
            }
            _ => {}
        }
    }
    let third_spec = tasks.iter().find(|t| t.id == third).unwrap();
    assert_eq!(
        third_spec.spec.deps.len(),
        1,
        "stored with its dependencies"
    );
    let mut r = Sim::new(cfg());
    r.core.recover(
        r.now,
        Recovered {
            tasks,
            ..Default::default()
        },
    );
    r.drain();
    assert_ne!(r.status(third), Some(TaskStatus::Blocked));
    r.replace(keyed(5.0).after("parent"), false).unwrap();
    r.run_to_completion(500.0);
    assert_eq!(r.core.live_tasks(), 0);
}

#[test]
fn a_stored_replacement_follows_a_task_that_ends_while_it_is_recovered() {
    // The lost attempt was the task's last: it is dead-lettered during recovery, and the
    // submission stored behind it runs.
    let mut pool = PoolConfig::fixed("default", 1);
    pool.retry = RetryPolicy::none();
    let mut r = Sim::new(CoreConfig {
        pools: vec![pool],
        persist: true,
        ..Default::default()
    });
    r.core.recover(
        r.now,
        Recovered {
            tasks: vec![RecoveredTask {
                id: TaskId(1),
                seq: 1,
                arrival: Time::ZERO,
                spec: keyed(10.0),
                attempt: 1,
                releases: 0,
            }],
            pending: vec![RecoveredPending {
                id: TaskId(1),
                seq: 2,
                arrival: Time::ZERO,
                renew: false,
                spec: keyed(2.0),
            }],
            ..Default::default()
        },
    );
    r.drain();
    assert_eq!(r.status(TaskId(1)), Some(TaskStatus::DeadLettered));
    r.run_to_completion(50.0);
    let successor = r.try_submit(keyed(9.0)).unwrap().id;
    assert_ne!(successor, TaskId(1));
    assert_eq!(payloads(&r, successor), [2.0]);
    assert_eq!(r.core.snapshot(r.now).pools[0].pending(), 0);
}

#[test]
fn renewing_the_position_works_in_a_pool_that_shares_capacity_between_tenants() {
    for renew in [false, true] {
        let mut pool = PoolConfig::fixed("default", 1);
        pool.fairness = Some(Fairness::default());
        let mut s = Sim::with_pool(pool);
        s.submit(job(10.0).tenant("a"));
        let a = s.submit(keyed(1.0).tenant("a"));
        let b = s.submit(job(1.0).tenant("a"));
        s.replace(keyed(2.0).tenant("a"), renew).unwrap();
        let c = s.submit(job(1.0).tenant("a"));
        s.run_to_completion(100.0);
        let order = s.order()[1..].to_vec();
        if renew {
            assert_eq!(order, [b, a, c]);
        } else {
            assert_eq!(order, [a, b, c]);
        }
    }
}

// ------------------------------------------------------------------------------------------
// Refusals and bounds
// ------------------------------------------------------------------------------------------

#[test]
fn replacing_needs_a_key() {
    let mut s = one_worker();
    assert_eq!(
        s.replace(job(1.0), false),
        Err(SubmitError::Invalid("replace_queued needs a key"))
    );
    assert_eq!(s.core.live_tasks(), 0);
}

#[test]
fn a_stored_replacement_respects_the_bound_of_its_class_and_sheds_nothing() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.bounds.max_pending_per_class[Priority::High.index()] = Some(1);
    pool.overflow = OverflowPolicy::Reject;
    let mut s = Sim::with_pool(pool);
    let running = s.submit(keyed(10.0));
    s.submit(job(1.0).priority(Priority::High));
    // The high class is full: a stored submission of that class is refused, directly or by
    // overwriting one of another class, and what was stored stays.
    assert_eq!(
        s.replace(keyed(2.0).priority(Priority::High), false),
        Err(SubmitError::Overloaded)
    );
    s.replace(keyed(3.0).priority(Priority::Low), false)
        .unwrap();
    assert_eq!(
        s.replace(keyed(4.0).priority(Priority::High), false),
        Err(SubmitError::Overloaded)
    );
    s.run_to_completion(100.0);
    let successor = s.try_submit(keyed(9.0)).unwrap().id;
    assert_ne!(successor, running);
    assert_eq!(payloads(&s, successor), [3.0]);

    // With a shedding policy, a stored submission still never evicts other work, least of
    // all the task it waits for.
    let mut pool = PoolConfig::fixed("default", 1);
    pool.bounds.max_pending = Some(2);
    pool.overflow = OverflowPolicy::ShedOldest;
    pool.retry = RetryPolicy {
        max_attempts: 5,
        initial_backoff: secs(1.0),
        multiplier: 1.0,
        max_backoff: secs(1.0),
        jitter: false,
    };
    let mut s = Sim::with_pool(pool);
    let k = s.submit(
        TaskSpec::new(Job::flaky(1.0, 1))
            .key("k")
            .priority(Priority::Low),
    );
    s.run_until(t(1.5));
    s.submit(job(20.0).priority(Priority::High));
    s.run_until(t(3.0));
    let other = s.submit(job(1.0).priority(Priority::Low));
    // `k` failed once and waits in line again, behind the long task; the queue is full.
    assert_eq!(s.status(k), Some(TaskStatus::Ready));
    assert_eq!(
        s.replace(keyed(2.0).priority(Priority::Low), false),
        Err(SubmitError::Overloaded)
    );
    assert_eq!(s.status(k), Some(TaskStatus::Ready));
    assert_eq!(s.status(other), Some(TaskStatus::Ready));
}

#[test]
fn a_replacement_that_differs_in_placement_is_refused() {
    let mut cfg = CoreConfig {
        pools: vec![
            PoolConfig::fixed("default", 1),
            PoolConfig::fixed("other", 1),
        ],
        resources: vec![ResourceConfig::semaphore("gpu", 2)],
        ..Default::default()
    };
    cfg.pools[0].unknown_dependency = UnknownDependency::Await { timeout: None };
    let mut s = Sim::new(cfg);
    s.submit(job(10.0));
    let id = s.submit(keyed(1.0).resource("gpu", 1).after("parent"));
    let base = || keyed(2.0).resource("gpu", 1).after("parent");
    let refused = |s: &mut Sim, spec: TaskSpec<Job>| match s.replace(spec, false) {
        Err(SubmitError::ReplaceMismatch(what)) => what,
        other => panic!("{other:?}"),
    };
    assert_eq!(refused(&mut s, base().pool("other")), "the pool differs");
    // The tenant is checked before anything else about the task is revealed.
    assert_eq!(
        refused(&mut s, keyed(2.0).tenant("acme").pool("other")),
        "the tenant differs"
    );
    assert_eq!(
        refused(&mut s, keyed(2.0).resource("gpu", 2).after("parent")),
        "the resources differ"
    );
    assert_eq!(
        refused(&mut s, keyed(2.0).resource("gpu", 1)),
        "the dependencies differ"
    );
    assert_eq!(
        refused(&mut s, base().label("zone", "a")),
        "the worker labels differ"
    );
    assert_eq!(
        refused(&mut s, base().fixed_start(FixedStart::at(t(500.0)))),
        "a replacement cannot have a fixed start"
    );
    assert!(matches!(
        s.replace(base().pool("nope"), false),
        Err(SubmitError::UnknownPool(_))
    ));
    // Nothing changed, and a matching replacement still works.
    assert_eq!(s.core.totals().superseded, 0);
    assert_eq!(s.replace(base(), false).unwrap().id, id);

    let fixed = s.submit(job(1.0).key("fixed").fixed_start(FixedStart::at(t(500.0))));
    assert_eq!(
        s.replace(job(2.0).key("fixed"), false),
        Err(SubmitError::ReplaceMismatch("the task has a fixed start"))
    );
    assert_eq!(s.status(fixed), Some(TaskStatus::Scheduled));
}

#[test]
fn a_stored_replacement_holds_a_place_in_the_queue_bounds() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.bounds.max_pending = Some(2);
    pool.overflow = OverflowPolicy::Reject;
    let mut s = Sim::with_pool(pool);
    let running = s.submit(keyed(10.0));
    s.submit(job(1.0));
    // One place left: the stored replacement takes it, and overwriting it takes no more.
    assert_eq!(
        s.replace(keyed(2.0), false).unwrap().outcome,
        SubmitOutcome::Pending
    );
    assert_eq!(
        s.replace(keyed(3.0), false).unwrap().outcome,
        SubmitOutcome::Pending
    );
    assert_eq!(s.try_submit(job(1.0)), Err(SubmitError::Overloaded));
    // A replacement that finds the queue full is refused, and the task is untouched.
    let other = s.core.snapshot(s.now).pools[0].pending();
    assert_eq!(other, 2);
    s.run_to_completion(100.0);
    assert_eq!(payloads(&s, running), [10.0]);
    assert_eq!(s.core.snapshot(s.now).pools[0].pending(), 0);

    let mut pool = PoolConfig::fixed("default", 1);
    pool.bounds.max_pending = Some(1);
    pool.overflow = OverflowPolicy::Reject;
    let mut s = Sim::with_pool(pool);
    s.submit(keyed(10.0));
    s.submit(job(1.0));
    assert_eq!(s.replace(keyed(2.0), false), Err(SubmitError::Overloaded));
    // A replacement in place needs no room: the task already has its place.
    let mut pool = PoolConfig::fixed("default", 1);
    pool.bounds.max_pending = Some(1);
    pool.overflow = OverflowPolicy::Reject;
    let mut s = Sim::with_pool(pool);
    s.submit(job(10.0));
    s.submit(keyed(1.0));
    assert_eq!(
        s.replace(keyed(2.0), false).unwrap().outcome,
        SubmitOutcome::Replaced
    );
}

// ------------------------------------------------------------------------------------------
// Events and durability
// ------------------------------------------------------------------------------------------

#[test]
fn replacements_are_reported_as_events_and_mutations_not_as_statuses() {
    let mut s = one_worker();
    s.core.set_emit_transitions(true);
    let blocker = s.submit(keyed(10.0).key("b"));
    let queued = s.submit(keyed(1.0));
    s.events.clear();
    s.persisted.clear();

    s.replace(keyed(2.0).priority(Priority::High), false)
        .unwrap();
    assert_eq!(s.events.len(), 1);
    let e = &s.events[0];
    assert_eq!(
        (e.id, e.kind, e.status, e.priority),
        (
            queued,
            EventKind::Replaced,
            TaskStatus::Ready,
            Priority::High
        )
    );
    let [Mutation::Replaced { id, spec, .. }] = &s.persisted[..] else {
        panic!("{:?}", s.persisted);
    };
    assert_eq!(
        (*id, spec.payload.dur, spec.priority),
        (queued, 2.0, Priority::High)
    );

    s.events.clear();
    s.persisted.clear();
    s.replace(job(3.0).key("b"), true).unwrap();
    let e = &s.events[0];
    assert_eq!(
        (e.id, e.kind, e.status),
        (blocker, EventKind::Replaced, TaskStatus::Running)
    );
    let [
        Mutation::PendingReplacement {
            id, renew, spec, ..
        },
    ] = &s.persisted[..]
    else {
        panic!("{:?}", s.persisted);
    };
    assert_eq!((*id, *renew, spec.payload.dur), (blocker, true, 3.0));

    // When the running task ends: its status, then the successor, in one batch.
    s.events.clear();
    s.persisted.clear();
    s.run_until(t(10.0));
    let ended = s
        .persisted
        .iter()
        .position(|m| matches!(m, Mutation::Status { id, status: TaskStatus::Succeeded, .. } if *id == blocker))
        .expect("the task's end is persisted");
    let Some(Mutation::Submitted {
        id: successor,
        spec,
        ..
    }) = s.persisted.get(ended + 1)
    else {
        panic!("the successor follows the end: {:?}", s.persisted);
    };
    assert_eq!(spec.key.as_deref(), Some("b"));
    // It is marked with the task it follows.
    assert!(
        spec.tags
            .iter()
            .any(|(k, v)| &**k == "cronoq.follows" && **v == *blocker.to_string())
    );
    let ev = s
        .events
        .iter()
        .find(|e| e.kind == EventKind::Successor)
        .expect("a successor event");
    assert_eq!((ev.id, ev.key.as_deref()), (*successor, Some("b")));
    assert_eq!(
        ev.message.as_deref(),
        Some(format!("follows task {blocker}").as_str())
    );
}

#[test]
fn without_transition_events_a_replacement_emits_no_event() {
    let mut s = one_worker();
    s.submit(job(10.0));
    s.submit(keyed(1.0));
    s.events.clear();
    s.replace(keyed(2.0), false).unwrap();
    assert!(s.events.is_empty());
}

#[test]
fn a_stored_replacement_survives_recovery() {
    let cfg = || CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    };
    let mut s = Sim::new(cfg());
    let first = s.submit(keyed(10.0));
    let queued = s.submit(job(1.0).key("q"));
    s.replace(keyed(2.0), false).unwrap();
    s.replace(job(5.0).key("q"), false).unwrap();

    // Rebuild the engine from what was persisted, as a store would hand it back: the first
    // task was running, so it comes back with its lost attempt counted.
    let mut tasks: Vec<RecoveredTask<Job>> = Vec::new();
    let mut pending = Vec::new();
    for m in &s.persisted {
        match m {
            Mutation::Submitted {
                id,
                seq,
                arrival,
                spec,
            } => tasks.push(RecoveredTask {
                id: *id,
                seq: *seq,
                arrival: *arrival,
                spec: spec.clone(),
                attempt: u32::from(*id == first),
                releases: 0,
            }),
            Mutation::Replaced {
                id,
                seq,
                arrival,
                spec,
            } => {
                let t = tasks.iter_mut().find(|t| t.id == *id).unwrap();
                (t.seq, t.arrival, t.spec) = (*seq, *arrival, spec.clone());
            }
            Mutation::PendingReplacement {
                id,
                seq,
                arrival,
                renew,
                spec,
            } => pending.push(RecoveredPending {
                id: *id,
                seq: *seq,
                arrival: *arrival,
                renew: *renew,
                spec: spec.clone(),
            }),
            _ => {}
        }
    }
    let mut r = Sim::new(cfg());
    r.core.recover(
        r.now,
        Recovered {
            tasks,
            pending,
            ..Default::default()
        },
    );
    r.drain();
    r.run_to_completion(200.0);
    assert_eq!(payloads(&r, first), [10.0]);
    assert_eq!(payloads(&r, queued), [5.0]);
    let successor = r.try_submit(keyed(9.0)).unwrap().id;
    assert_ne!(successor, first);
    assert_eq!(payloads(&r, successor), [2.0]);
}
