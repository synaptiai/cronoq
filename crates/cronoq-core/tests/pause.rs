//! Pause and resume: a pool, one of its classes or one of its ordering keys stops being
//! offered to workers while its tasks stay queued, in place.

mod common;

use common::*;
use cronoq_core::*;

fn sim(workers: u32) -> Sim {
    Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", workers)],
        persist: true,
        ..Default::default()
    })
}

fn job(d: f64) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d))
}

const P: PoolId = PoolId(0);

fn pause(s: &mut Sim, target: PauseTarget, reason: &str) -> bool {
    let r = s.core.pause(s.now, P, &target, reason).unwrap();
    s.drain();
    r
}

fn resume(s: &mut Sim, target: PauseTarget) -> bool {
    let r = s.core.resume(s.now, P, &target).unwrap();
    s.drain();
    r
}

fn last_pause(s: &Sim) -> Option<(PauseTarget, Option<String>)> {
    s.persisted.iter().rev().find_map(|m| match m {
        Mutation::Paused { target, reason, .. } => {
            Some((target.clone(), reason.as_ref().map(|r| r.to_string())))
        }
        _ => None,
    })
}

#[test]
fn a_paused_pool_keeps_its_queue_and_runs_nothing_new() {
    let mut s = sim(2);
    let running = s.submit(job(5.0));
    assert!(pause(&mut s, PauseTarget::Pool, "deploy"));
    // Submissions are accepted and wait; the running attempt is not interrupted.
    let a = s.submit(job(1.0).priority(Priority::Critical));
    let b = s.submit(job(1.0));
    s.run_until(t(20.0));
    assert_eq!(s.status(running), Some(TaskStatus::Succeeded));
    assert_eq!(s.status(a), Some(TaskStatus::Ready));
    assert_eq!(s.order(), vec![running]);
    let snap = s.core.snapshot(s.now);
    let p = &snap.pools[0];
    assert_eq!(p.pauses.pool.as_deref(), Some("deploy"));
    assert_eq!((p.paused_tasks, p.pending()), (2, 2));
    assert_eq!(p.pauses.holds(Priority::Low).map(|r| &**r), Some("deploy"));
    assert_eq!(
        last_pause(&s),
        Some((PauseTarget::Pool, Some("deploy".into())))
    );

    // Pausing again is not a new pause; another reason replaces the first.
    let stored = s.persisted.len();
    assert!(!pause(&mut s, PauseTarget::Pool, "deploy"));
    assert_eq!(s.persisted.len(), stored);
    assert!(!pause(&mut s, PauseTarget::Pool, "deploy 2"));
    assert_eq!(s.core.pauses(P).unwrap().pool.as_deref(), Some("deploy 2"));

    // Resumed: the tasks run in the order they would have, with the age they have.
    assert!(resume(&mut s, PauseTarget::Pool));
    assert!(!resume(&mut s, PauseTarget::Pool));
    assert_eq!(last_pause(&s), Some((PauseTarget::Pool, None)));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![running, a, b]);
    assert_eq!(s.start_of(a), Some(t(20.0)));
    assert!(s.core.snapshot(s.now).pools[0].pauses.is_empty());
}

#[test]
fn a_paused_class_holds_that_class_only() {
    let mut s = sim(1);
    assert!(pause(
        &mut s,
        PauseTarget::Class(Priority::Normal),
        "backfill off"
    ));
    let busy = s.submit(job(1.0).priority(Priority::Critical));
    let normal = s.submit(job(1.0));
    let low = s.submit(job(1.0).priority(Priority::Low));
    let high = s.submit(job(1.0).priority(Priority::High));
    s.run_until(t(10.0));
    assert_eq!(s.order(), vec![busy, high, low]);
    assert_eq!(s.status(normal), Some(TaskStatus::Ready));
    assert_eq!(s.core.snapshot(s.now).pools[0].paused_tasks, 1);
    // A task moved into the paused class is held; one moved out of it runs.
    let hold = s.submit(job(3.0).priority(Priority::Critical));
    let other = s.submit(job(1.0).priority(Priority::Low));
    s.update(other, TaskPatch::default().priority(Priority::Normal))
        .unwrap();
    s.update(normal, TaskPatch::default().priority(Priority::Low))
        .unwrap();
    s.run_until(t(20.0));
    assert_eq!(s.order(), vec![busy, high, low, hold, normal]);
    // The pool and its classes are paused and resumed apart.
    assert!(pause(&mut s, PauseTarget::Pool, "all"));
    assert!(resume(&mut s, PauseTarget::Pool));
    s.run_until(t(30.0));
    assert_eq!(s.status(other), Some(TaskStatus::Ready));
    assert!(resume(&mut s, PauseTarget::Class(Priority::Normal)));
    s.run_to_completion(20.0);
    assert_eq!(s.status(other), Some(TaskStatus::Succeeded));
}

#[test]
fn a_class_pause_goes_by_the_class_a_task_runs_at() {
    let mut s = sim(1);
    let busy = s.submit(job(1.0));
    // A low head of an ordering key, lifted to high by the task behind it.
    let head = s.submit(job(1.0).ordering_key("k").priority(Priority::Low));
    let high = s.submit(job(1.0).ordering_key("k").priority(Priority::High));
    assert!(pause(
        &mut s,
        PauseTarget::Class(Priority::High),
        "hold high"
    ));
    let low = s.submit(job(1.0).priority(Priority::Low));
    s.run_until(t(10.0));
    // The head runs at high: it is held, though its own class is low and low is not paused.
    assert_eq!(s.order(), vec![busy, low]);
    assert!(resume(&mut s, PauseTarget::Class(Priority::High)));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![busy, low, head, high]);
}

#[test]
fn a_paused_ordering_key_waits_and_other_keys_run() {
    let mut s = sim(1);
    let busy = s.submit(job(1.0));
    // The head is already queued when its key is paused: it waits again.
    let a1 = s.submit(job(1.0).ordering_key("a"));
    let a2 = s.submit(job(1.0).ordering_key("a"));
    let b1 = s.submit(job(1.0).ordering_key("b"));
    assert!(pause(
        &mut s,
        PauseTarget::Ordering("a".into()),
        "customer asked"
    ));
    s.run_until(t(10.0));
    assert_eq!(s.order(), vec![busy, b1]);
    let i = s.core.ordering(P, "a").unwrap();
    assert_eq!(i.paused.as_deref(), Some("customer asked"));
    assert_eq!((i.tasks, i.head, i.halted), (2, Some(a1), false));
    assert_eq!(
        s.core.paused_orderings(P, 10),
        vec![("a".into(), "customer asked".into())]
    );
    let snap = s.core.snapshot(s.now);
    assert_eq!(
        (snap.pools[0].ordering.paused, snap.pools[0].paused_tasks),
        (1, 2)
    );
    assert!(resume(&mut s, PauseTarget::Ordering("a".into())));
    s.run_to_completion(20.0);
    assert_eq!(s.order(), vec![busy, b1, a1, a2]);
    assert_eq!(s.core.ordering(P, "a"), None);

    // A key can be paused before it has tasks; what is submitted then waits.
    assert!(pause(
        &mut s,
        PauseTarget::Ordering("later".into()),
        "not yet"
    ));
    let l = s.submit(job(1.0).ordering_key("later"));
    s.run_until(s.now + secs(5.0));
    assert_eq!(s.status(l), Some(TaskStatus::Ready));
    // The same call resumes a key that is both paused and halted by a failed task.
    let p = s
        .core
        .pause(s.now, P, &PauseTarget::Ordering("h".into()), "x");
    assert_eq!(p, Ok(true));
    assert!(resume(&mut s, PauseTarget::Ordering("h".into())));
    let bad = s.submit(
        TaskSpec::new(Job::broken(1.0))
            .ordering_key("h")
            .retry(RetryPolicy::none())
            .halt_on_failure(),
    );
    let next = s.submit(job(1.0).ordering_key("h"));
    s.run_until(s.now + secs(5.0));
    assert_eq!(s.status(bad), Some(TaskStatus::Failed));
    assert!(pause(&mut s, PauseTarget::Ordering("h".into()), "looking"));
    assert!(resume(&mut s, PauseTarget::Ordering("h".into())));
    assert!(resume(&mut s, PauseTarget::Ordering("later".into())));
    s.run_to_completion(20.0);
    assert_eq!(s.status(next), Some(TaskStatus::Succeeded));
    assert_eq!(s.status(l), Some(TaskStatus::Succeeded));
    assert!(s.core.paused_orderings(P, 10).is_empty());
}

#[test]
fn retries_wait_and_fixed_starts_are_not_held() {
    let mut s = sim(2);
    let retry = RetryPolicy {
        max_attempts: 2,
        initial_backoff: secs(1.0),
        multiplier: 1.0,
        max_backoff: secs(1.0),
        jitter: false,
    };
    let flaky = s.submit(TaskSpec::new(Job::flaky(1.0, 1)).retry(retry));
    assert!(pause(&mut s, PauseTarget::Pool, "maintenance"));
    let fixed = s.submit(job(1.0).fixed_start(FixedStart::at(t(5.0))));
    s.run_until(t(20.0));
    // The first attempt was running and finished (it failed); its retry waits.
    assert_eq!(s.started.iter().filter(|x| x.id == flaky).count(), 1);
    assert!(!s.status(flaky).unwrap().is_terminal());
    // The fixed start ran at its time.
    assert_eq!(s.start_of(fixed), Some(t(5.0)));
    assert!(resume(&mut s, PauseTarget::Pool));
    s.run_to_completion(20.0);
    assert_eq!(s.status(flaky), Some(TaskStatus::Succeeded));
}

#[test]
fn paused_tasks_count_toward_the_bounds_and_can_be_cancelled() {
    let mut s = Sim::with_pool(PoolConfig {
        bounds: Bounds {
            max_pending: Some(2),
            ..Default::default()
        },
        overflow: OverflowPolicy::Reject,
        ..PoolConfig::fixed("default", 1)
    });
    assert!(pause(&mut s, PauseTarget::Pool, "x"));
    let a = s.submit(job(1.0));
    let _b = s.submit(job(1.0));
    assert_eq!(s.try_submit(job(1.0)), Err(SubmitError::Overloaded));
    assert!(s.core.cancel(s.now, &TaskRef::Id(a)));
    s.drain();
    assert!(s.try_submit(job(1.0)).is_ok());
    assert_eq!(
        s.core.pause(s.now, PoolId(9), &PauseTarget::Pool, "x"),
        Err(OrderingError::UnknownPool)
    );
    assert_eq!(
        s.core.resume(s.now, PoolId(9), &PauseTarget::Pool),
        Err(OrderingError::UnknownPool)
    );
}

#[test]
fn pauses_survive_recovery() {
    let mut r = sim(2);
    let pause_of = |target, reason: &str| RecoveredPause {
        pool: "default".into(),
        target,
        reason: reason.into(),
    };
    let task = |id: u64, spec: TaskSpec<Job>| RecoveredTask {
        id: TaskId(id),
        seq: id,
        arrival: Time::ZERO,
        spec,
        attempt: 0,
    };
    r.core.recover(
        r.now,
        Recovered {
            tasks: vec![
                task(1, job(1.0).priority(Priority::Low)),
                task(2, job(1.0).ordering_key("k")),
                task(3, job(1.0)),
            ],
            paused: vec![
                pause_of(PauseTarget::Class(Priority::Low), "low off"),
                pause_of(PauseTarget::Ordering("k".into()), "key off"),
                RecoveredPause {
                    pool: "gone".into(),
                    target: PauseTarget::Pool,
                    reason: "x".into(),
                },
            ],
            next_id: 4,
            ..Default::default()
        },
    );
    r.drain();
    r.run_until(t(10.0));
    assert_eq!(r.order(), vec![TaskId(3)]);
    let pauses = r.core.pauses(P).unwrap();
    assert_eq!(
        pauses.classes[Priority::Low.index()].as_deref(),
        Some("low off")
    );
    assert_eq!(
        r.core.ordering(P, "k").unwrap().paused.as_deref(),
        Some("key off")
    );
    // The pause of a pool that is gone is dropped from the store.
    assert!(r.persisted.iter().any(|m| matches!(
        m,
        Mutation::Paused { pool, reason: None, .. } if &**pool == "gone"
    )));
    assert!(resume(&mut r, PauseTarget::Class(Priority::Low)));
    assert!(resume(&mut r, PauseTarget::Ordering("k".into())));
    r.run_to_completion(20.0);
    assert_eq!(r.core.live_tasks(), 0);
}

#[test]
fn a_paused_pool_does_not_ask_for_workers() {
    let mut p = PoolConfig::new("default").workers(0, 0, 4);
    p.autoscale.as_mut().unwrap().interval = secs(0.5);
    let mut s = Sim::with_pool(p);
    assert!(pause(&mut s, PauseTarget::Pool, "x"));
    for _ in 0..20 {
        s.submit(job(1.0));
    }
    s.run_until(t(30.0));
    assert!(s.started.is_empty());
    assert!(
        s.scales.iter().all(|(_, _, n)| *n == 0),
        "scaled up for held work: {:?}",
        s.scales
    );
    assert!(resume(&mut s, PauseTarget::Pool));
    s.run_to_completion(120.0);
    assert_eq!(s.core.live_tasks(), 0);
}
