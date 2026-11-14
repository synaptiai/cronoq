//! Releasing an attempt: the executor gives the task back without a verdict. The task keeps
//! its place, is offered again after a delay, and the attempt does not count against the
//! retry policy.

mod common;

use common::*;
use cronoq_core::*;

fn sim(workers: u32) -> Sim {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", workers)],
        persist: true,
        ..Default::default()
    });
    // Attempts are ended by the test, not by the clock.
    s.hang = true;
    s
}

fn job() -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(1.0)).retry(RetryPolicy {
        max_attempts: 2,
        initial_backoff: secs(1.0),
        multiplier: 1.0,
        max_backoff: secs(1.0),
        jitter: false,
    })
}

/// The lease of the attempt of `id` that started last.
fn lease_of(s: &Sim, id: TaskId) -> LeaseId {
    s.started.iter().rev().find(|x| x.id == id).unwrap().lease
}

fn release(s: &mut Sim, id: TaskId, delay: f64) -> bool {
    let lease = lease_of(s, id);
    let r = s.core.release(s.now, lease, secs(delay));
    s.drain();
    r
}

fn end(s: &mut Sim, id: TaskId, outcome: Outcome) {
    let lease = lease_of(s, id);
    assert!(s.core.complete(s.now, lease, outcome));
    s.drain();
}

fn attempts(s: &Sim, id: TaskId) -> Vec<u32> {
    s.started
        .iter()
        .filter(|x| x.id == id)
        .map(|x| x.attempt)
        .collect()
}

#[test]
fn a_released_task_runs_again_after_the_delay_without_using_a_retry() {
    let mut s = sim(1);
    let id = s.submit(job());
    assert!(release(&mut s, id, 5.0));
    // It waits, like a task that is retried, and is not offered before the delay is over.
    assert_eq!(s.status(id), Some(TaskStatus::Retrying));
    s.run_until(t(4.0));
    assert_eq!(attempts(&s, id), [1]);
    s.run_until(t(5.0));
    // Every attempt has its own number.
    assert_eq!(attempts(&s, id), [1, 2]);
    assert!(matches!(
        s.persisted
            .iter()
            .rev()
            .find(|m| matches!(m, Mutation::Released { .. })),
        Some(Mutation::Released {
            attempt: 1,
            releases: 1,
            ..
        })
    ));
    assert_eq!(s.core.totals().releases, 1);
    // The retry policy allows two attempts: both are still to come.
    end(&mut s, id, Outcome::retryable("flaky"));
    assert_eq!(s.status(id), Some(TaskStatus::Retrying));
    s.run_until(t(10.0));
    assert_eq!(attempts(&s, id), [1, 2, 3]);
    end(&mut s, id, Outcome::retryable("flaky"));
    assert_eq!(s.status(id), Some(TaskStatus::DeadLettered));
    // A released attempt is not a failure.
    let snap = s.core.snapshot(s.now);
    assert_eq!((snap.pools[0].failed, snap.totals.retries), (2, 1));
}

#[test]
fn a_released_task_keeps_its_place_in_line() {
    let mut s = sim(1);
    let first = s.submit(job());
    let second = s.submit(job());
    let third = s.submit(job());
    assert!(release(&mut s, first, 0.0));
    // Released without a delay: it is ahead of the tasks submitted after it.
    assert_eq!(s.order(), vec![first, first]);
    assert!(release(&mut s, first, 2.0));
    // While it waits, the next task runs; when it is back it is ahead of the third.
    assert_eq!(s.order(), vec![first, first, second]);
    s.run_until(t(3.0));
    end(&mut s, second, Outcome::Success);
    assert_eq!(s.order(), vec![first, first, second, first]);
    end(&mut s, first, Outcome::Success);
    assert_eq!(s.order().last(), Some(&third));
    // In an ordering key the released head keeps the key: the task behind it waits.
    let mut s = sim(2);
    let head = s.submit(job().ordering_key("k"));
    let behind = s.submit(job().ordering_key("k"));
    assert!(release(&mut s, head, 5.0));
    s.run_until(t(4.0));
    assert_eq!(s.start_of(behind), None);
    s.run_until(t(5.0));
    end(&mut s, head, Outcome::Success);
    assert_eq!(s.order(), vec![head, head, behind]);
}

#[test]
fn releases_are_limited_and_then_count_as_failures() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.max_releases = 2;
    let mut s = Sim::with_pool(pool);
    s.hang = true;
    let id = s.submit(job());
    for _ in 0..2 {
        assert!(release(&mut s, id, 0.0));
    }
    assert_eq!(attempts(&s, id), [1, 2, 3]);
    // The third release is a retryable failure: it uses the first of two attempts, and the
    // delay asked for is the least it waits.
    assert!(release(&mut s, id, 30.0));
    assert_eq!(s.status(id), Some(TaskStatus::Retrying));
    s.run_until(t(29.0));
    assert_eq!(attempts(&s, id).len(), 3);
    s.run_until(t(30.0));
    assert_eq!(attempts(&s, id).len(), 4);
    assert!(release(&mut s, id, 0.0));
    assert_eq!(s.status(id), Some(TaskStatus::DeadLettered));
    let msg = s.events.last().unwrap().message.clone().unwrap();
    assert!(msg.contains("released more than 2 times"), "{msg}");

    // A task's own limit replaces the pool's; zero means a release is always a failure.
    let more = s.submit(job().max_releases(3));
    for _ in 0..3 {
        assert!(release(&mut s, more, 0.0));
    }
    assert_eq!(attempts(&s, more).len(), 4);
    let none = s.submit(job().max_releases(0));
    end(&mut s, more, Outcome::Success);
    assert!(release(&mut s, none, 0.0));
    assert_eq!(s.core.totals().releases, 5);
}

#[test]
fn releases_made_while_the_task_is_paused_do_not_count() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.max_releases = 1;
    let mut s = Sim::with_pool(pool);
    s.hang = true;
    let id = s.submit(job());
    // The pool is paused while the attempt runs: the worker gives the task back.
    s.core
        .pause(s.now, PoolId(0), &PauseTarget::Pool, "deploy")
        .unwrap();
    assert!(release(&mut s, id, 0.0));
    assert_eq!(attempts(&s, id), [1], "held by the pause");
    s.core.resume(s.now, PoolId(0), &PauseTarget::Pool).unwrap();
    s.drain();
    // The limit of one is still unused.
    assert!(release(&mut s, id, 0.0));
    assert_eq!(s.status(id), Some(TaskStatus::Running));
    assert_eq!(attempts(&s, id), [1, 2, 3]);
    // Now it is used up: the next release is a failure.
    assert!(release(&mut s, id, 0.0));
    assert_eq!(s.status(id), Some(TaskStatus::Retrying));
    assert_eq!(s.core.snapshot(s.now).pools[0].failed, 1);
}

#[test]
fn a_release_of_an_unknown_lease_changes_nothing() {
    let mut s = sim(1);
    let id = s.submit(job());
    let lease = lease_of(&s, id);
    assert!(s.core.complete(s.now, lease, Outcome::Success));
    assert!(!s.core.release(s.now, lease, secs(1.0)));
    assert!(!s.core.release(s.now, LeaseId(999), secs(1.0)));
    s.drain();
    assert_eq!(s.status(id), Some(TaskStatus::Succeeded));
    // A released task can be cancelled while it waits, and its resources are free.
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 2)],
        resources: vec![ResourceConfig::semaphore("gpu", 1)],
        ..Default::default()
    });
    s.hang = true;
    let a = s.submit(job().resource("gpu", 1));
    let b = s.submit(job().resource("gpu", 1));
    assert!(release(&mut s, a, 60.0));
    assert_eq!(
        s.start_of(b),
        Some(Time::ZERO),
        "the resource was given back"
    );
    assert!(s.core.cancel(s.now, &TaskRef::Id(a)));
    s.drain();
    assert_eq!(s.status(a), Some(TaskStatus::Cancelled));
}

#[test]
fn released_attempts_are_recovered_and_do_not_count_after_a_failover() {
    let mut r = sim(1);
    r.core.recover(
        r.now,
        Recovered {
            tasks: vec![
                // Three attempts, two of them released, and the last one was running when
                // the leader was lost: that one counts, so one of two attempts is used.
                RecoveredTask {
                    id: TaskId(1),
                    seq: 1,
                    arrival: Time::ZERO,
                    spec: job(),
                    attempt: 3,
                    releases: 2,
                },
                // The same without releases: three attempts of two, nothing left.
                RecoveredTask {
                    id: TaskId(2),
                    seq: 2,
                    arrival: Time::ZERO,
                    spec: job(),
                    attempt: 3,
                    releases: 0,
                },
            ],
            next_id: 3,
            ..Default::default()
        },
    );
    r.drain();
    assert_eq!(r.status(TaskId(2)), Some(TaskStatus::DeadLettered));
    assert_eq!(r.status(TaskId(1)), Some(TaskStatus::Retrying));
    r.run_until(t(5.0));
    assert_eq!(attempts(&r, TaskId(1)), [4]);
    end(&mut r, TaskId(1), Outcome::retryable("x"));
    assert_eq!(r.status(TaskId(1)), Some(TaskStatus::DeadLettered));
}

#[test]
fn releases_of_a_fixed_start_task_count_while_its_pool_is_paused() {
    // A pause does not hold a task with a fixed start, so its releases are not free.
    for target in [PauseTarget::Pool, PauseTarget::Class(Priority::Normal)] {
        let mut s = Sim::with_pool(PoolConfig {
            max_releases: 1,
            ..PoolConfig::fixed("default", 1)
        });
        s.hang = true;
        s.core.pause(s.now, PoolId(0), &target, "x").unwrap();
        let id = s.submit(job().fixed_start(FixedStart::at(t(1.0))));
        s.run_until(t(2.0));
        assert_eq!(s.status(id), Some(TaskStatus::Running));
        assert!(release(&mut s, id, 0.0));
        assert_eq!(attempts(&s, id), vec![1, 2]);
        // The second release is over the limit: a failure, which uses the one retry.
        assert!(release(&mut s, id, 0.0));
        assert_eq!(s.core.snapshot(s.now).totals.releases, 1);
        s.run_until(t(10.0));
        assert_eq!(attempts(&s, id), vec![1, 2, 3]);
        assert!(release(&mut s, id, 0.0));
        assert_eq!(s.status(id), Some(TaskStatus::DeadLettered));
        s.core.check_invariants();
    }
}

#[test]
fn a_replacement_brings_its_own_release_limit() {
    let mut s = sim(1);
    let blocker = s.submit(job());
    let id = s.submit(job().key("k").max_releases(0));
    let r = s.replace(job().key("k").max_releases(3), false).unwrap();
    assert_eq!(r.outcome, SubmitOutcome::Replaced);
    end(&mut s, blocker, Outcome::Success);
    assert!(release(&mut s, id, 0.0));
    assert_eq!(s.status(id), Some(TaskStatus::Running));
    assert_eq!(attempts(&s, id), vec![1, 2]);
    assert_eq!(s.core.snapshot(s.now).totals.releases, 1);
}

#[test]
fn a_released_task_keeps_its_place_among_the_tenants_of_a_fair_pool() {
    let mut s = Sim::with_pool(PoolConfig {
        fairness: Some(Fairness::weights([("a", 1.0), ("b", 1.0)])),
        ..PoolConfig::fixed("default", 1)
    });
    s.hang = true;
    let first = s.submit(job().tenant("a"));
    let second = s.submit(job().tenant("a"));
    let third = s.submit(job().tenant("b"));
    // Released without a delay, it is back before anything else, and its tenant is not
    // charged a second time: `third` still runs before `second`.
    assert!(release(&mut s, first, 0.0));
    assert_eq!(s.order(), vec![first, first]);
    end(&mut s, first, Outcome::Success);
    end(&mut s, third, Outcome::Success);
    assert_eq!(s.order(), vec![first, first, third, second]);
    s.core.check_invariants();
}
