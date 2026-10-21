mod common;

use common::*;
use cronoq_core::*;

fn one_worker() -> Sim {
    Sim::with_pool(PoolConfig::fixed("default", 1))
}

fn job(d: f64) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d))
}

// ------------------------------------------------------------------------------------------
// Priority, aging, deadlines
// ------------------------------------------------------------------------------------------

#[test]
fn dispatches_by_priority_then_fifo() {
    let mut s = one_worker();
    let blocker = s.submit(job(1.0));
    let l = s.submit(job(1.0).priority(Priority::Low));
    let n1 = s.submit(job(1.0).priority(Priority::Normal));
    let h = s.submit(job(1.0).priority(Priority::High));
    let n2 = s.submit(job(1.0).priority(Priority::Normal));
    let c = s.submit(job(1.0).priority(Priority::Critical));
    s.run_to_completion(100.0);
    assert_eq!(s.order(), vec![blocker, c, h, n1, n2, l]);
}

#[test]
fn aging_lets_old_low_beat_fresh_normal_but_not_high() {
    let mut s = one_worker();
    s.submit(job(500.0));
    let low = s.submit(job(1.0).priority(Priority::Low));
    s.run_until(t(499.0));
    let normal = s.submit(job(1.0).priority(Priority::Normal));
    let high = s.submit(job(1.0).priority(Priority::High));
    s.run_to_completion(100.0);
    let order = s.order();
    // high (1000) > aged low (10 + 800 cap) > fresh normal (100 + 2)
    assert_eq!(&order[1..], &[high, low, normal]);
}

#[test]
fn low_priority_is_not_starved_by_normal_flood() {
    let mut s = one_worker();
    let low = s.submit(job(1.0).priority(Priority::Low));
    // A continuous flood of fresh normal tasks, one per second.
    for i in 0..200 {
        s.submit(job(1.0));
        s.run_until(t(i as f64 + 1.0));
        if s.start_of(low).is_some() {
            break;
        }
    }
    let started = s.start_of(low).expect("low task must start");
    // Low overtakes fresh normal once 10 + 2w > 100 + 2*age_of_normal_head; bounded well below 100s.
    assert!(started < t(100.0), "low started at {started:?}");
}

#[test]
fn imminent_deadline_boosts_low_priority() {
    let mut s = one_worker();
    s.submit(job(1.0));
    let n = s.submit(job(1.0));
    let urgent = s.submit(job(1.0).priority(Priority::Low).deadline(t(2.5)));
    s.run_to_completion(50.0);
    assert_eq!(&s.order()[1..], &[urgent, n]);
}

#[test]
fn deadline_tasks_run_in_latest_start_order_and_overdue_stays_most_urgent() {
    let mut s = one_worker();
    s.submit(job(10.0));
    // At t=10: a is overdue by 8s, b is about to become overdue. Overdue must not lose (prototype bug #4).
    let b = s.submit(job(1.0).deadline(t(11.2)));
    let a = s.submit(job(1.0).deadline(t(3.0)));
    let c = s.submit(job(1.0).deadline(t(100.0)));
    s.run_to_completion(50.0);
    assert_eq!(&s.order()[1..], &[a, b, c]);
}

// ------------------------------------------------------------------------------------------
// Fixed start
// ------------------------------------------------------------------------------------------

#[test]
fn fixed_start_holds_a_worker_and_starts_on_time() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.default_duration = secs(3.0);
    let mut s = Sim::with_pool(pool);
    for _ in 0..10 {
        s.submit(job(3.0));
    }
    let f = s.submit(job(1.0).fixed_start(FixedStart::at(t(10.0))));
    s.run_to_completion(200.0);
    let start = s.start_of(f).unwrap();
    assert!(
        start.diff_secs(t(10.0)).abs() <= 1.0,
        "fixed task started at {start:?}"
    );
    // The recorded start is the actual dispatch time, and counts as a hit.
    let snap = s.core.snapshot(s.now);
    assert_eq!(snap.pools[0].fixed_start_hit_rate, 1.0);
    assert_eq!(s.status(f), Some(TaskStatus::Succeeded));
}

#[test]
fn reservations_are_bounded_by_pool_capacity() {
    // max 4 workers, reservable fraction 0.5 => 2 concurrent reservations.
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 4));
    let fs = |at: f64| {
        job(5.0)
            .duration_hint(secs(5.0))
            .fixed_start(FixedStart::at(t(at)))
    };
    assert!(s.try_submit(fs(100.0)).is_ok());
    assert!(s.try_submit(fs(101.0)).is_ok());
    assert_eq!(
        s.try_submit(fs(102.0)).unwrap_err(),
        SubmitError::ReservationConflict
    );
    assert!(
        s.try_submit(fs(120.0)).is_ok(),
        "non-overlapping reservation must be accepted"
    );
    // Single-worker pools still accept one reservation at a time.
    let mut one = one_worker();
    assert!(one.try_submit(fs(10.0)).is_ok());
    assert_eq!(
        one.try_submit(fs(12.0)).unwrap_err(),
        SubmitError::ReservationConflict
    );
}

#[test]
fn miss_policies() {
    for (policy, expected) in [
        (MissPolicy::Skip, TaskStatus::Missed),
        (MissPolicy::Fail, TaskStatus::Failed),
        (MissPolicy::RunLate, TaskStatus::Succeeded),
    ] {
        let mut s = one_worker();
        s.submit(job(100.0));
        s.submit(job(1.0)); // regular work queued behind the blocker
        let f = s.submit(job(1.0).fixed_start(FixedStart {
            at: t(5.0),
            tolerance: secs(1.0),
            on_miss: policy,
        }));
        s.run_to_completion(300.0);
        assert_eq!(s.status(f), Some(expected), "{policy:?}");
        if policy == MissPolicy::RunLate {
            // Runs right after the blocker, ahead of the queued regular task.
            assert_eq!(s.order()[1], f);
        }
    }
}

// ------------------------------------------------------------------------------------------
// Dependencies
// ------------------------------------------------------------------------------------------

#[test]
fn dag_respects_order_and_conditions() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 4));
    let a = s.submit(job(1.0).key("a"));
    let b = s.submit(job(1.0).key("b").after("a"));
    let c = s.submit(job(1.0).after("b").after(a));
    s.run_to_completion(50.0);
    let (sa, sb, sc) = (
        s.start_of(a).unwrap(),
        s.start_of(b).unwrap(),
        s.start_of(c).unwrap(),
    );
    assert!(sb >= sa + secs(1.0) && sc >= sb + secs(1.0));
    assert_eq!(s.status(c), Some(TaskStatus::Succeeded));
}

#[test]
fn parent_failure_skips_on_success_children_and_runs_handlers() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 4));
    let a = s.submit(TaskSpec::new(Job::broken(1.0)).key("a"));
    let b = s.submit(job(1.0).after("a"));
    let c = s.submit(job(1.0).after(b)); // transitively skipped
    let on_fail = s.submit(job(1.0).dep(Dep::on_failure("a")));
    let always = s.submit(job(1.0).dep(Dep::on_complete("a")));
    s.run_to_completion(50.0);
    assert_eq!(s.status(a), Some(TaskStatus::Failed));
    assert_eq!(s.status(b), Some(TaskStatus::Skipped));
    assert_eq!(s.status(c), Some(TaskStatus::Skipped));
    assert_eq!(s.status(on_fail), Some(TaskStatus::Succeeded));
    assert_eq!(s.status(always), Some(TaskStatus::Succeeded));
}

#[test]
fn dependency_on_long_finished_parent_is_resolved_from_retention() {
    // Prototype bug #1: completed tasks beyond 1000 entries were forgotten and children hung.
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 8));
    let first = s.submit(job(0.001).key("first"));
    for _ in 0..3000 {
        s.submit(job(0.001));
    }
    s.run_to_completion(100.0);
    assert_eq!(s.status(first), Some(TaskStatus::Succeeded));
    let child = s.submit(job(0.5).after("first"));
    s.run_to_completion(10.0);
    assert_eq!(s.status(child), Some(TaskStatus::Succeeded));
}

#[test]
fn unknown_dependencies_reject_or_await() {
    let mut s = one_worker();
    assert_eq!(
        s.try_submit(job(1.0).after("nope")).unwrap_err(),
        SubmitError::UnknownDependency("nope".into())
    );

    let mut pool = PoolConfig::fixed("default", 1);
    pool.unknown_dependency = UnknownDependency::Await {
        timeout: Some(secs(30.0)),
    };
    let mut s = Sim::with_pool(pool);
    let child = s.submit(job(1.0).key("child").after("parent"));
    let orphan = s.submit(job(1.0).after("never"));
    s.run_until(t(5.0));
    assert_eq!(s.status(child), Some(TaskStatus::Blocked));
    // A parent that depends on the child would close a cycle.
    assert_eq!(
        s.try_submit(job(1.0).key("parent").after("child"))
            .unwrap_err(),
        SubmitError::Cycle
    );
    let parent = s.submit(job(1.0).key("parent"));
    s.run_to_completion(100.0);
    assert!(s.start_of(child).unwrap() >= s.start_of(parent).unwrap() + secs(1.0));
    assert_eq!(s.status(orphan), Some(TaskStatus::Skipped));
}

#[test]
fn self_dependency_is_a_cycle() {
    let mut s = one_worker();
    assert_eq!(
        s.try_submit(job(1.0).key("x").after("x")).unwrap_err(),
        SubmitError::Cycle
    );
}

#[test]
fn batch_is_atomic_and_order_independent() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 2));
    let specs = vec![
        job(1.0).key("c").after("b"),
        job(1.0).key("b").after("a"),
        job(1.0).key("a"),
    ];
    let ids = s.core.submit_batch(s.now, specs).unwrap();
    s.drain();
    s.run_to_completion(20.0);
    let (a, b, c) = (ids[2].id, ids[1].id, ids[0].id);
    assert!(s.start_of(a).unwrap() < s.start_of(b).unwrap());
    assert!(s.start_of(b).unwrap() < s.start_of(c).unwrap());

    // A batch with a cycle or a conflict admits nothing.
    let before = s.core.totals().submitted;
    let err = s.core.submit_batch(
        s.now,
        vec![job(1.0).key("x").after("y"), job(1.0).key("y").after("x")],
    );
    assert_eq!(err.unwrap_err().1, SubmitError::Cycle);
    let fx = |at: f64| job(1.0).fixed_start(FixedStart::at(t(at)));
    let err = s
        .core
        .submit_batch(s.now, vec![job(1.0), fx(100.0), fx(100.5)]);
    assert_eq!(err.unwrap_err(), (2, SubmitError::ReservationConflict));
    assert_eq!(s.core.totals().submitted, before);
    s.core.check_invariants();
}

#[test]
fn keys_deduplicate_submissions() {
    let mut s = one_worker();
    let a = s.try_submit(job(1.0).key("k")).unwrap();
    let b = s.try_submit(job(1.0).key("k")).unwrap();
    assert!(!a.duplicate && b.duplicate && a.id == b.id);
    s.run_to_completion(10.0);
    let c = s.try_submit(job(1.0).key("k")).unwrap();
    assert!(c.duplicate, "retained terminal keys still deduplicate");
}

// ------------------------------------------------------------------------------------------
// Failures, retries, timeouts, leases, cancellation
// ------------------------------------------------------------------------------------------

#[test]
fn retries_until_success_or_dead_letter() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 2));
    let ok = s.submit(TaskSpec::new(Job::flaky(1.0, 2)));
    let dead = s.submit(TaskSpec::new(Job::flaky(1.0, 10)));
    let perm = s.submit(TaskSpec::new(Job::broken(1.0)));
    s.run_to_completion(500.0);
    assert_eq!(s.status(ok), Some(TaskStatus::Succeeded));
    assert_eq!(s.status(dead), Some(TaskStatus::DeadLettered));
    assert_eq!(s.status(perm), Some(TaskStatus::Failed));
    let attempts = |id| s.started.iter().filter(|x| x.id == id).count();
    assert_eq!((attempts(ok), attempts(dead), attempts(perm)), (3, 3, 1));
    // Backoff: attempt 2 starts within [1s end, 1s + 1s backoff]; attempt 3 within +2s.
    let starts: Vec<_> = s
        .started
        .iter()
        .filter(|x| x.id == ok)
        .map(|x| x.at)
        .collect();
    assert!(starts[1].diff_secs(starts[0]) <= 2.0 + 1e-6);
    assert!(starts[2].diff_secs(starts[1]) <= 3.0 + 1e-6);
}

#[test]
fn retry_after_hint_is_honoured() {
    let mut s = one_worker();
    let id = s.submit(TaskSpec::new(Job::ok(100.0)));
    s.run_until(t(0.5));
    let lease = LeaseId(1);
    s.core.complete(
        s.now,
        lease,
        Outcome::Failure {
            retryable: true,
            retry_after: Some(secs(30.0)),
            message: "429".into(),
        },
    );
    s.drain();
    s.run_until(t(30.0));
    assert_eq!(s.started.iter().filter(|x| x.id == id).count(), 1);
    s.run_until(t(31.0));
    assert_eq!(s.started.iter().filter(|x| x.id == id).count(), 2);
}

#[test]
fn timeouts_abort_and_retry() {
    let mut s = one_worker();
    s.hang = true;
    let id = s.submit(job(1.0).timeout(secs(2.0)).retry(RetryPolicy {
        max_attempts: 2,
        jitter: false,
        ..Default::default()
    }));
    s.run_until(t(60.0));
    assert_eq!(
        s.aborts
            .iter()
            .filter(|a| a.1 == AbortReason::Timeout)
            .count(),
        2
    );
    assert_eq!(s.status(id), Some(TaskStatus::DeadLettered));
}

#[test]
fn leases_expire_without_heartbeat() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.lease_ttl = Some(secs(5.0));
    pool.retry = RetryPolicy::none();
    let mut s = Sim::with_pool(pool);
    s.hang = true;
    let id = s.submit(job(1.0));
    s.run_until(t(4.0));
    assert!(s.core.heartbeat(s.now, LeaseId(1)));
    s.run_until(t(8.0));
    assert!(s.aborts.is_empty(), "heartbeat extended the lease");
    s.run_until(t(9.5));
    assert_eq!(s.aborts, vec![(LeaseId(1), AbortReason::LeaseExpired)]);
    assert_eq!(s.status(id), Some(TaskStatus::DeadLettered));
    assert!(
        !s.core.complete(s.now, LeaseId(1), Outcome::Success),
        "stale lease is ignored"
    );
}

#[test]
fn cancel_queued_and_running() {
    let mut s = one_worker();
    let running = s.submit(job(10.0).key("r"));
    let queued = s.submit(job(1.0));
    let child = s.submit(job(1.0).after("r"));
    assert!(s.core.cancel(s.now, &TaskRef::Id(queued)));
    assert!(s.core.cancel(s.now, &"r".into()));
    s.drain();
    assert_eq!(s.aborts, vec![(LeaseId(1), AbortReason::Cancelled)]);
    assert_eq!(s.status(running), Some(TaskStatus::Cancelled));
    assert_eq!(s.status(queued), Some(TaskStatus::Cancelled));
    assert_eq!(s.status(child), Some(TaskStatus::Skipped));
    assert!(!s.core.cancel(s.now, &TaskRef::Id(queued)));
}

// ------------------------------------------------------------------------------------------
// Resources
// ------------------------------------------------------------------------------------------

#[test]
fn semaphore_limits_concurrency_across_workers() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 4)],
        resources: vec![ResourceConfig::semaphore("gpu", 1)],
        ..Default::default()
    });
    let a = s.submit(job(2.0).resource("gpu", 1));
    let b = s.submit(job(2.0).resource("gpu", 1));
    let free = s.submit(job(2.0));
    s.run_to_completion(20.0);
    assert_eq!(s.start_of(a), Some(t(0.0)));
    assert_eq!(
        s.start_of(free),
        Some(t(0.0)),
        "unconstrained work is not blocked behind the parked task"
    );
    assert_eq!(s.start_of(b), Some(t(2.0)));
}

#[test]
fn token_bucket_rate_limits() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 8)],
        resources: vec![ResourceConfig::token_bucket("api", 2.0, 1)],
        ..Default::default()
    });
    let ids: Vec<_> = (0..5)
        .map(|_| s.submit(job(0.1).resource("api", 1)))
        .collect();
    s.run_to_completion(20.0);
    for (i, id) in ids.iter().enumerate() {
        let st = s.start_of(*id).unwrap().as_secs_f64();
        assert!(
            (st - i as f64 * 0.5).abs() < 0.01,
            "task {i} started at {st}"
        );
    }
    assert_eq!(
        s.try_submit(job(1.0).resource("api", 5)).unwrap_err(),
        SubmitError::ResourceExceedsCapacity("api".into())
    );
}

// ------------------------------------------------------------------------------------------
// Backpressure
// ------------------------------------------------------------------------------------------

#[test]
fn bounded_queue_policies() {
    let bounded = |policy| {
        let mut p = PoolConfig::fixed("default", 1);
        p.bounds.max_pending = Some(2);
        p.overflow = policy;
        Sim::with_pool(p)
    };
    let mut s = bounded(OverflowPolicy::Wait);
    s.submit(job(1.0)); // dispatched immediately
    s.submit(job(1.0));
    s.submit(job(1.0));
    assert_eq!(s.try_submit(job(1.0)).unwrap_err(), SubmitError::Full);
    s.run_until(t(1.0));
    assert_eq!(s.space_available, 1);
    assert!(s.try_submit(job(1.0)).is_ok());

    let mut s = bounded(OverflowPolicy::Reject);
    s.submit(job(1.0));
    s.submit(job(1.0));
    s.submit(job(1.0));
    assert_eq!(s.try_submit(job(1.0)).unwrap_err(), SubmitError::Overloaded);

    let mut s = bounded(OverflowPolicy::ShedLowest);
    s.submit(job(1.0));
    let low = s.submit(job(1.0).priority(Priority::Low));
    s.submit(job(1.0).priority(Priority::Normal));
    let high = s.try_submit(job(1.0).priority(Priority::High)).unwrap();
    assert_eq!(s.status(low), Some(TaskStatus::Shed));
    assert_eq!(s.status(high.id), Some(TaskStatus::Ready));
    // Nothing lower than Low to shed for a Low submission.
    assert_eq!(
        s.try_submit(job(1.0).priority(Priority::Low)).unwrap_err(),
        SubmitError::Overloaded
    );
}

// ------------------------------------------------------------------------------------------
// Recurring
// ------------------------------------------------------------------------------------------

#[test]
fn recurring_cron_runs_each_occurrence_and_skips_overlaps() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 4));
    s.core
        .add_recurring(
            s.now,
            RecurringSpec {
                name: "tick".into(),
                cron: "*/10 * * * * *".into(),
                tz: None,
                template: job(1.0).kind("tick"),
                overlap: OverlapPolicy::Skip,
                tolerance: secs(1.0),
                on_miss: MissPolicy::RunLate,
            },
        )
        .unwrap();
    s.core
        .add_recurring(
            s.now,
            RecurringSpec {
                name: "slow".into(),
                cron: "*/10 * * * * *".into(),
                tz: None,
                template: job(15.0).kind("slow"),
                overlap: OverlapPolicy::Skip,
                tolerance: secs(1.0),
                on_miss: MissPolicy::RunLate,
            },
        )
        .unwrap();
    s.drain();
    s.run_until(t(55.0));
    let ticks: Vec<f64> = s
        .started
        .iter()
        .filter(|x| x.kind == "tick")
        .map(|x| x.at.as_secs_f64())
        .collect();
    assert_eq!(ticks, vec![10.0, 20.0, 30.0, 40.0, 50.0]);
    let slow: Vec<f64> = s
        .started
        .iter()
        .filter(|x| x.kind == "slow")
        .map(|x| x.at.as_secs_f64())
        .collect();
    assert_eq!(
        slow,
        vec![10.0, 30.0, 50.0],
        "occurrences overlapping a running one are skipped"
    );
    assert!(s.core.remove_recurring(s.now, "tick"));
    assert!(
        s.core
            .add_recurring(
                s.now,
                RecurringSpec {
                    name: "bad".into(),
                    cron: "not a cron".into(),
                    tz: None,
                    template: job(1.0),
                    overlap: OverlapPolicy::Allow,
                    tolerance: secs(1.0),
                    on_miss: MissPolicy::Skip,
                }
            )
            .is_err()
    );
}

// ------------------------------------------------------------------------------------------
// Autoscaling
// ------------------------------------------------------------------------------------------

#[test]
fn autoscaler_grows_under_backlog_and_shrinks_when_idle() {
    let mut pool = PoolConfig::new("default").workers(1, 1, 8);
    pool.autoscale = Some(AutoscaleConfig {
        drain_time: secs(10.0),
        stabilization_window: secs(10.0),
        cooldown: secs(2.0),
        ..Default::default()
    });
    let mut s = Sim::with_pool(pool);
    for _ in 0..100 {
        s.submit(job(1.0));
    }
    s.run_to_completion(200.0);
    let peak = s.scales.iter().map(|x| x.2).max().unwrap();
    assert_eq!(peak, 8);
    let done_at = s
        .events
        .iter()
        .filter(|e| e.status == TaskStatus::Succeeded)
        .map(|e| e.at)
        .max()
        .unwrap();
    assert!(
        done_at < t(30.0),
        "100 x 1s tasks finished at {done_at:?} (1 worker would need 100s)"
    );
    s.run_until(s.now + secs(60.0));
    assert_eq!(
        s.core.pool_capacity(PoolId(0)),
        1,
        "scaled back to min when idle"
    );
    // Prototype bug #5: idle time must not trigger scale-up.
    let mut prev = 8;
    for &(at, _, d) in &s.scales {
        if at > done_at + secs(1.0) {
            assert!(d <= prev, "scaled up while idle at {at:?}");
            prev = d;
        }
    }
}

#[test]
fn autoscaler_prewarms_for_reservations() {
    let mut pool = PoolConfig::new("default").workers(1, 1, 8);
    pool.autoscale = Some(AutoscaleConfig {
        reservation_horizon: secs(30.0),
        ..Default::default()
    });
    let mut s = Sim::with_pool(pool);
    s.submit(job(10.0)); // keep one worker busy
    s.submit(
        job(1.0)
            .duration_hint(secs(5.0))
            .fixed_start(FixedStart::at(t(20.0))),
    );
    s.run_until(t(5.0));
    assert!(
        s.core.pool_capacity(PoolId(0)) >= 2,
        "capacity pre-warmed ahead of the reservation"
    );
}

// ------------------------------------------------------------------------------------------
// Durations, metrics
// ------------------------------------------------------------------------------------------

#[test]
fn snapshot_reports_quality_metrics() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 2));
    for _ in 0..10 {
        s.submit(job(1.0));
    }
    s.run_to_completion(30.0);
    let snap = s.core.snapshot(s.now);
    let p = &snap.pools[0];
    assert_eq!(p.succeeded, 10);
    assert_eq!(p.priority_adherence, 1.0);
    assert!(p.utilization > 0.5, "utilization {}", p.utilization);
    assert!(p.wait_p90 > secs(3.0));
    assert!((p.run_p50.as_secs_f64() - 1.0).abs() < 0.1);
    assert_eq!(snap.totals.succeeded, 10);
}

#[test]
fn priority_inversions_are_counted() {
    let mut s = one_worker();
    s.submit(job(500.0));
    s.submit(job(1.0).priority(Priority::Low));
    s.run_until(t(499.0));
    s.submit(job(1.0).priority(Priority::Normal));
    s.run_to_completion(50.0);
    let p = &s.core.snapshot(s.now).pools[0];
    // The aged low task ran while a fresh normal task was waiting: one inversion out of three.
    assert_eq!(p.dispatched, 2, "window only covers the last minute");
    assert!(p.priority_adherence < 1.0);
}

#[test]
fn learned_durations_feed_estimates() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.default_duration = secs(1.0);
    let mut s = Sim::with_pool(pool);
    for _ in 0..20 {
        s.submit(job(10.0).kind("slow"));
    }
    s.run_to_completion(400.0);
    // With ~10s learned for "slow", a reservation for a "slow" task covers ~10s.
    let at = s.now + secs(100.0);
    s.submit(job(10.0).kind("slow").fixed_start(FixedStart::at(at)));
    let r = s.try_submit(
        job(1.0)
            .kind("fast")
            .fixed_start(FixedStart::at(at + secs(6.0))),
    );
    assert_eq!(r.unwrap_err(), SubmitError::ReservationConflict);
}

// ------------------------------------------------------------------------------------------
// Persistence and recovery
// ------------------------------------------------------------------------------------------

fn rebuild(log: &[Mutation<Job>]) -> Recovered<Job> {
    use std::collections::BTreeMap;
    let mut tasks: BTreeMap<TaskId, RecoveredTask<Job>> = BTreeMap::new();
    let mut terminal = Vec::new();
    let mut next_id = 0;
    for m in log {
        match m {
            Mutation::Submitted {
                id,
                seq,
                arrival,
                spec,
            } => {
                next_id = next_id.max(id.0 + 1);
                tasks.insert(
                    *id,
                    RecoveredTask {
                        id: *id,
                        seq: *seq,
                        arrival: *arrival,
                        spec: spec.clone(),
                        attempt: 0,
                    },
                );
            }
            Mutation::Status {
                id,
                status,
                attempt,
                at,
                ..
            } => {
                if status.is_terminal() {
                    let t = tasks.remove(id).unwrap();
                    terminal.push(RecoveredTerminal {
                        id: *id,
                        key: t.spec.key.clone(),
                        status: *status,
                        at: *at,
                    });
                } else if let Some(t) = tasks.get_mut(id) {
                    t.attempt = *attempt;
                }
            }
            _ => {}
        }
    }
    Recovered {
        tasks: tasks.into_values().collect(),
        terminal,
        recurring: vec![],
        next_id,
    }
}

#[test]
fn recovers_from_mutation_log() {
    let cfg = CoreConfig {
        persist: true,
        pools: vec![PoolConfig::fixed("default", 1)],
        ..Default::default()
    };
    let mut s = Sim::new(cfg.clone());
    let a = s.submit(job(1.0).key("a"));
    let b = s.submit(job(5.0).key("b"));
    let c = s.submit(job(1.0).key("c").after("b"));
    s.run_until(t(2.0)); // a done, b running, c blocked
    assert_eq!(s.status(a), Some(TaskStatus::Succeeded));

    // Crash: rebuild a fresh engine from the log.
    let rec = rebuild(&s.persisted);
    let mut s2 = Sim::new(cfg);
    s2.now = t(2.0);
    s2.core.recover(s2.now, rec);
    s2.drain();
    assert_eq!(s2.core.status(&"a".into()), Some(TaskStatus::Succeeded));
    s2.run_to_completion(100.0);
    assert_eq!(s2.status(b), Some(TaskStatus::Succeeded));
    assert_eq!(s2.status(c), Some(TaskStatus::Succeeded));
    // b was running when we crashed: its lost attempt counts, so it runs as attempt 2.
    assert_eq!(s2.started.iter().find(|x| x.id == b).unwrap().attempt, 2);
    let fresh = s2.submit(job(1.0));
    assert!(fresh.0 > c.0, "ids continue after recovery");
}

#[test]
fn recovered_running_task_respects_retry_budget() {
    let cfg = CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        ..Default::default()
    };
    let mut s = Sim::new(cfg);
    let spec = |n| {
        job(1.0).retry(RetryPolicy {
            max_attempts: n,
            ..Default::default()
        })
    };
    let rec = Recovered {
        tasks: vec![
            RecoveredTask {
                id: TaskId(1),
                seq: 0,
                arrival: Time::ZERO,
                spec: spec(2),
                attempt: 2,
            },
            RecoveredTask {
                id: TaskId(2),
                seq: 1,
                arrival: Time::ZERO,
                spec: spec(3),
                attempt: 2,
            },
        ],
        terminal: vec![],
        recurring: vec![],
        next_id: 3,
    };
    s.core.recover(s.now, rec);
    s.drain();
    s.run_to_completion(60.0);
    assert_eq!(s.status(TaskId(1)), Some(TaskStatus::DeadLettered));
    assert_eq!(s.status(TaskId(2)), Some(TaskStatus::Succeeded));
    assert_eq!(
        s.started
            .iter()
            .filter(|x| x.id == TaskId(2))
            .map(|x| x.attempt)
            .collect::<Vec<_>>(),
        vec![3]
    );
}
