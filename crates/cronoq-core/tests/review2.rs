//! Regression tests for the second correctness review of the engine (each failed before its fix),
//! plus tests for the v0.2 engine items.

mod common;

use std::time::Duration;

use common::*;
use cronoq_core::*;

fn job(d: f64) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d))
}

fn no_jitter() -> RetryPolicy {
    RetryPolicy {
        jitter: false,
        ..Default::default()
    }
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

fn dispatched(core: &mut Core<()>) -> bool {
    let mut any = false;
    while let Some(a) = core.poll_action() {
        any |= matches!(a, Action::Dispatch(_));
    }
    any
}

// --- #1: calls that fire timers must also dispatch ------------------------------------------

#[test]
fn snapshot_dispatches_tasks_made_ready_by_timers() {
    let mut core: Core<()> = Core::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        ..Default::default()
    });
    core.submit(Time::ZERO, TaskSpec::new(()).retry(no_jitter()))
        .unwrap();
    let Some(Action::Dispatch(d)) = core.poll_action() else {
        panic!()
    };
    core.complete(t(0.1), d.lease, Outcome::retryable("x"));
    while core.poll_action().is_some() {}
    let wake = core.next_wakeup().unwrap(); // retry at 1.1s
    let _ = core.snapshot(wake);
    assert!(
        dispatched(&mut core),
        "Ready task with a free slot was not dispatched"
    );
}

#[test]
fn heartbeat_dispatches_tasks_made_ready_by_timers() {
    let mut p = PoolConfig::fixed("default", 2);
    p.lease_ttl = Some(secs(30.0));
    let mut core: Core<()> = Core::new(CoreConfig {
        pools: vec![p],
        ..Default::default()
    });
    core.submit(Time::ZERO, TaskSpec::new(())).unwrap();
    core.submit(Time::ZERO, TaskSpec::new(()).retry(no_jitter()))
        .unwrap();
    let mut leases = vec![];
    while let Some(a) = core.poll_action() {
        if let Action::Dispatch(d) = a {
            leases.push(d.lease);
        }
    }
    core.complete(t(0.1), leases[1], Outcome::retryable("x"));
    while core.poll_action().is_some() {}
    core.heartbeat(t(1.2), leases[0]); // B's retry timer (1.1s) fires in here
    assert!(dispatched(&mut core));
}

#[test]
fn remove_recurring_dispatches_tasks_made_ready_by_timers() {
    let mut core: Core<()> = Core::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        ..Default::default()
    });
    core.submit(Time::ZERO, TaskSpec::new(()).retry(no_jitter()))
        .unwrap();
    let Some(Action::Dispatch(d)) = core.poll_action() else {
        panic!()
    };
    core.complete(t(0.1), d.lease, Outcome::retryable("x"));
    while core.poll_action().is_some() {}
    core.remove_recurring(t(2.0), "nothing");
    assert!(dispatched(&mut core));
}

// --- #2: tolerance 0 is not an automatic miss ------------------------------------------------

#[test]
fn zero_tolerance_fixed_start_runs_on_time() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 2));
    let id = sim.submit(job(1.0).fixed_start(FixedStart {
        at: t(10.0),
        tolerance: Duration::ZERO,
        on_miss: MissPolicy::Skip,
    }));
    sim.run_until(t(20.0));
    assert_eq!(
        sim.start_of(id),
        Some(t(10.0)),
        "status={:?}",
        sim.status(id)
    );
}

// --- #3: a fixed task that started on time is not "missed" during its retry ------------------

#[test]
fn retried_fixed_task_is_not_marked_missed() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 1));
    let pol = RetryPolicy {
        initial_backoff: Duration::from_millis(300),
        ..no_jitter()
    };
    let fixed = sim.submit(
        TaskSpec::new(Job::flaky(0.1, 1))
            .retry(pol)
            .fixed_start(FixedStart {
                at: t(10.0),
                tolerance: secs(1.0),
                on_miss: MissPolicy::Skip,
            }),
    );
    sim.now = t(9.95);
    sim.submit(job(5.0)); // grabs the slot as soon as the first attempt fails
    sim.run_until(t(30.0));
    assert_eq!(sim.start_of(fixed), Some(t(10.0)));
    assert_eq!(
        sim.status(fixed),
        Some(TaskStatus::Succeeded),
        "attempt 2 should still run"
    );
}

// --- #4: recovered children of parents that left retention -----------------------------------

#[test]
fn recovered_child_with_expired_key_parent_runs() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 1));
    let child = job(1.0).after("parent-done-long-ago");
    sim.core.recover(
        t(0.0),
        Recovered {
            tasks: vec![rec_task(10, 10, child)],
            next_id: 11,
            ..Default::default()
        },
    );
    sim.drain();
    sim.run_until(t(100.0));
    assert_eq!(sim.status(TaskId(10)), Some(TaskStatus::Succeeded));
}

#[test]
fn recovered_on_failure_child_with_expired_parent_runs() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 1));
    let child = job(1.0).dep(Dep::on_failure(TaskId(3)));
    sim.core.recover(
        t(0.0),
        Recovered {
            tasks: vec![rec_task(10, 10, child)],
            next_id: 11,
            ..Default::default()
        },
    );
    sim.drain();
    sim.run_until(t(100.0));
    assert_eq!(sim.status(TaskId(10)), Some(TaskStatus::Succeeded));
}

#[test]
fn recovered_child_still_waits_for_live_parent() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 1));
    let parent = job(5.0).key("p");
    let child = job(1.0).after("p");
    sim.core.recover(
        t(0.0),
        Recovered {
            tasks: vec![rec_task(1, 1, parent), rec_task(2, 2, child)],
            next_id: 3,
            ..Default::default()
        },
    );
    sim.drain();
    sim.run_to_completion(100.0);
    assert!(sim.start_of(TaskId(2)).unwrap() >= t(5.0));
}

// --- #5: cron occurrences are whole-second aligned -------------------------------------------

#[test]
fn cron_occurrences_ignore_subsecond_offset() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 4));
    sim.now = t(0.3);
    sim.core
        .add_recurring(
            sim.now,
            RecurringSpec {
                name: "r".into(),
                cron: "0 * * * * *".into(),
                tz: None,
                template: job(1.0),
                overlap: OverlapPolicy::Allow,
                tolerance: secs(1.0),
                on_miss: MissPolicy::Skip,
            },
        )
        .unwrap();
    sim.drain();
    sim.run_until(t(200.0));
    let starts: Vec<Time> = sim.started.iter().map(|s| s.at).collect();
    assert_eq!(starts, vec![t(60.0), t(120.0), t(180.0)]);
}

// --- #6: invalid configuration and extreme inputs never hang or panic ------------------------

#[test]
fn autoscale_interval_zero_does_not_hang() {
    let mut p = PoolConfig::new("default");
    p.autoscale.as_mut().unwrap().interval = Duration::ZERO;
    let mut core: Core<()> = Core::new(CoreConfig {
        pools: vec![p],
        ..Default::default()
    });
    core.tick(Time::ZERO);
    core.tick(t(10.0));
    assert!(core.next_wakeup().unwrap() > t(10.0));
}

#[test]
fn token_bucket_extreme_rates_are_safe() {
    for rate in [1e-30, f64::NAN, -1.0, f64::INFINITY, 0.0] {
        let mut core: Core<()> = Core::new(CoreConfig {
            pools: vec![PoolConfig::fixed("default", 4)],
            resources: vec![ResourceConfig::token_bucket("tb", rate, 1)],
            ..Default::default()
        });
        core.submit(Time::ZERO, TaskSpec::new(()).resource("tb", 1))
            .unwrap();
        core.submit(Time::ZERO, TaskSpec::new(()).resource("tb", 1))
            .unwrap();
        core.tick(t(5.0));
        core.check_invariants();
    }
}

#[test]
fn backoff_with_extreme_values_saturates() {
    let p = RetryPolicy {
        max_attempts: 10,
        initial_backoff: secs(1.0),
        multiplier: 1e10,
        max_backoff: Duration::MAX,
        jitter: false,
    };
    assert!(p.backoff(4) >= Duration::from_secs(1_000_000));
    let p = RetryPolicy {
        initial_backoff: Duration::MAX,
        max_backoff: Duration::MAX,
        ..p
    };
    let _ = p.backoff(2);
    let p = RetryPolicy {
        multiplier: f64::NAN,
        ..p
    };
    let _ = p.backoff(3);
}

// --- #7: running fixed tasks are not counted twice by the autoscaler ------------------------

#[test]
fn autoscaler_does_not_double_count_running_fixed_task() {
    let mut p = PoolConfig::new("default").workers(0, 1, 10);
    p.autoscale.as_mut().unwrap().stabilization_window = secs(1.0);
    p.autoscale.as_mut().unwrap().cooldown = secs(1.0);
    let mut sim = Sim::with_pool(p);
    sim.submit(
        job(100.0)
            .duration_hint(secs(100.0))
            .fixed_start(FixedStart::at(t(2.0))),
    );
    sim.run_until(t(50.0));
    let s = sim.core.snapshot(sim.now);
    assert_eq!(s.pools[0].busy, 1);
    assert_eq!(s.pools[0].desired, 1, "scales={:?}", sim.scales);
}

// --- #8: a fixed task submitted after its window applies its miss policy ---------------------

#[test]
fn stale_fixed_submit_applies_miss_policy() {
    for (policy, expected) in [
        (MissPolicy::Skip, TaskStatus::Missed),
        (MissPolicy::Fail, TaskStatus::Failed),
        (MissPolicy::RunLate, TaskStatus::Succeeded),
    ] {
        let mut sim = Sim::with_pool(PoolConfig::fixed("default", 1));
        sim.now = t(100.0);
        let id = sim.submit(job(1.0).fixed_start(FixedStart {
            at: t(10.0),
            tolerance: secs(1.0),
            on_miss: policy,
        }));
        sim.run_until(t(200.0));
        assert_eq!(sim.status(id), Some(expected), "{policy:?}");
    }
}

// --- #9: retries do not count as late fixed starts --------------------------------------------

#[test]
fn hit_rate_ignores_retries() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 2));
    let pol = RetryPolicy {
        initial_backoff: secs(5.0),
        ..no_jitter()
    };
    sim.submit(
        TaskSpec::new(Job::flaky(0.1, 1))
            .retry(pol)
            .fixed_start(FixedStart::at(t(10.0))),
    );
    sim.run_until(t(30.0));
    let s = sim.core.snapshot(sim.now);
    assert_eq!(s.pools[0].fixed_start_hit_rate, 1.0);
}

// --- #10: remove + re-add does not duplicate a materialized occurrence -----------------------

#[test]
fn remove_then_readd_recurring_does_not_duplicate_occurrence() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 4));
    let spec = RecurringSpec {
        name: "r".into(),
        cron: "0 * * * * *".into(),
        tz: None,
        template: job(1.0),
        overlap: OverlapPolicy::Skip,
        tolerance: secs(1.0),
        on_miss: MissPolicy::Skip,
    };
    sim.core.add_recurring(Time::ZERO, spec.clone()).unwrap();
    sim.drain();
    sim.run_until(t(59.5)); // occurrence at 60s already materialized
    assert!(sim.core.remove_recurring(sim.now, "r"));
    sim.core.add_recurring(sim.now, spec).unwrap();
    sim.drain();
    sim.run_until(t(130.0));
    let starts: Vec<Time> = sim.started.iter().map(|s| s.at).collect();
    assert_eq!(starts, vec![t(60.0), t(120.0)]);
}

// --- #11: ShedOldest/ShedLowest consider deadline queues --------------------------------------

#[test]
fn shed_oldest_considers_low_deadline_tasks() {
    let mut p = PoolConfig::fixed("default", 0);
    p.bounds.max_pending = Some(1);
    p.overflow = OverflowPolicy::ShedOldest;
    let mut sim = Sim::with_pool(p);
    let low = sim.submit(job(1.0).priority(Priority::Low).deadline(t(1000.0)));
    let r = sim.try_submit(job(1.0).priority(Priority::High));
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(sim.status(low), Some(TaskStatus::Shed));
}

#[test]
fn shed_lowest_picks_newest_deadline_task() {
    let mut p = PoolConfig::fixed("default", 0);
    p.bounds.max_pending = Some(2);
    p.overflow = OverflowPolicy::ShedLowest;
    let mut sim = Sim::with_pool(p);
    // Older task has the later deadline; the newest must be the victim.
    let older = sim.submit(job(1.0).priority(Priority::Low).deadline(t(1000.0)));
    let newer = sim.submit(job(1.0).priority(Priority::Low).deadline(t(10.0)));
    sim.try_submit(job(1.0).priority(Priority::High)).unwrap();
    assert_eq!(sim.status(newer), Some(TaskStatus::Shed));
    assert_eq!(sim.status(older), Some(TaskStatus::Ready));
}

// --- #12: histogram top bucket ---------------------------------------------------------------

#[test]
fn histogram_top_bucket_quantile() {
    let mut h = Histogram::default();
    h.record(Duration::MAX);
    assert!(h.quantile(0.5) > Duration::from_secs(1 << 30));
}

// --- #13: DST behaviour (croner) is documented by these tests --------------------------------

fn utc(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

fn recurring_starts(cron: &str, tz: &str, epoch: &str, hours: f64) -> Vec<String> {
    let e = utc(epoch);
    let mut sim = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 4)],
        epoch_unix_nanos: e,
        ..Default::default()
    });
    sim.core
        .add_recurring(
            Time::ZERO,
            RecurringSpec {
                name: "r".into(),
                cron: cron.into(),
                tz: Some(tz.into()),
                template: job(1.0),
                overlap: OverlapPolicy::Allow,
                tolerance: secs(1.0),
                on_miss: MissPolicy::Skip,
            },
        )
        .unwrap();
    sim.drain();
    sim.run_until(t(hours * 3600.0));
    sim.started
        .iter()
        .map(|s| chrono::DateTime::from_timestamp_nanos(e + s.at.0 as i64).to_rfc3339())
        .collect()
}

#[test]
fn dst_spring_forward_daily_job_runs_after_the_gap() {
    // 02:30 does not exist on 2026-03-08 in New York; that day's run moves to 03:00 local.
    let v = recurring_starts(
        "0 30 2 * * *",
        "America/New_York",
        "2026-03-06T12:00:00Z",
        24.0 * 5.0,
    );
    assert_eq!(v.len(), 5, "{v:?}");
    assert!(
        v.contains(&"2026-03-08T07:00:00+00:00".to_string()),
        "{v:?}"
    );
}

#[test]
fn dst_fall_back_daily_job_runs_once() {
    // 01:30 occurs twice on 2026-11-01 in New York; the job runs once that day.
    let v = recurring_starts(
        "0 30 1 * * *",
        "America/New_York",
        "2026-10-30T12:00:00Z",
        24.0 * 4.0,
    );
    assert_eq!(v.len(), 4, "{v:?}");
}

#[test]
fn dst_fall_back_hourly_job_runs_every_real_hour() {
    // On fall-back 01:00 (local) occurs twice in New York, at 05:00 and 06:00 UTC. An hourly
    // schedule runs in both, so there is no gap in UTC.
    let v = recurring_starts(
        "0 0 * * * *",
        "America/New_York",
        "2026-11-01T03:30:00Z",
        4.0,
    );
    assert_eq!(
        v,
        vec![
            "2026-11-01T04:00:00+00:00",
            "2026-11-01T05:00:00+00:00",
            "2026-11-01T06:00:00+00:00",
            "2026-11-01T07:00:00+00:00",
        ],
    );
}

#[test]
fn dst_spring_forward_hourly_job_is_continuous() {
    let v = recurring_starts(
        "0 0 * * * *",
        "America/New_York",
        "2026-03-08T05:30:00Z",
        4.0,
    );
    assert_eq!(v.len(), 4, "{v:?}");
}

// --- #14: draining due fixed tasks is not quadratic ------------------------------------------

#[test]
fn draining_due_tasks_scales_linearly() {
    fn run(n: usize) -> u64 {
        let mut p = PoolConfig::fixed("default", 1);
        p.reservable_fraction = 1e9;
        let mut core: Core<()> = Core::new(CoreConfig {
            pools: vec![p],
            ..Default::default()
        });
        for i in 0..n {
            core.submit(
                Time::ZERO,
                TaskSpec::new(())
                    .duration_hint(Duration::from_nanos(1))
                    .fixed_start(FixedStart {
                        at: Time(i as u64 % 1000),
                        tolerance: Duration::ZERO,
                        on_miss: MissPolicy::RunLate,
                    }),
            )
            .unwrap();
        }
        let mut leases = vec![];
        let mut now = Time(2000);
        let start = std::time::Instant::now();
        loop {
            while let Some(a) = core.poll_action() {
                if let Action::Dispatch(d) = a {
                    leases.push(d.lease);
                }
            }
            let Some(l) = leases.pop() else { break };
            now = Time(now.0 + 1);
            core.complete(now, l, Outcome::Success);
        }
        assert_eq!(core.totals().succeeded as usize, n);
        start.elapsed().as_micros() as u64
    }
    let a = run(2_000).max(1);
    let b = run(8_000);
    // 4x the tasks must not take ~16x the time (generous bound for debug builds / noise).
    assert!(b < a * 10, "2k: {a}us, 8k: {b}us");
}

// --- v0.2: shedding parked tasks ---------------------------------------------------------------

#[test]
fn shed_considers_parked_tasks() {
    let mut p = PoolConfig::fixed("default", 4);
    p.bounds.max_pending = Some(1);
    p.overflow = OverflowPolicy::ShedLowest;
    let mut sim = Sim::new(CoreConfig {
        pools: vec![p],
        resources: vec![ResourceConfig::semaphore("db", 1)],
        ..Default::default()
    });
    sim.submit(job(100.0).resource("db", 1)); // runs, holds the permit
    let parked = sim.submit(job(1.0).priority(Priority::Low).resource("db", 1));
    let r = sim.try_submit(job(1.0).priority(Priority::High));
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(sim.status(parked), Some(TaskStatus::Shed));
}

#[test]
fn fixed_start_tasks_are_never_shed() {
    let mut p = PoolConfig::fixed("default", 0);
    p.bounds.max_pending = Some(1);
    p.overflow = OverflowPolicy::ShedOldest;
    let mut sim = Sim::with_pool(p);
    let fixed = sim.submit(
        job(1.0)
            .priority(Priority::Low)
            .fixed_start(FixedStart::at(t(50.0))),
    );
    let r = sim.try_submit(job(1.0).priority(Priority::High));
    assert_eq!(r.unwrap_err(), SubmitError::Overloaded);
    assert_eq!(sim.status(fixed), Some(TaskStatus::Scheduled));
}

// --- v0.2: awaits are cleaned up --------------------------------------------------------------

#[test]
fn abandoned_awaits_are_cleaned_up() {
    let mut p = PoolConfig::fixed("default", 1);
    p.unknown_dependency = UnknownDependency::Await {
        timeout: Some(secs(5.0)),
    };
    let mut sim = Sim::with_pool(p);
    let a = sim.submit(job(1.0).after("ghost-a"));
    let b = sim.submit(job(1.0).after("ghost-b"));
    assert!(sim.core.cancel(sim.now, &TaskRef::Id(a)));
    sim.drain();
    sim.run_until(t(10.0)); // b times out
    assert_eq!(sim.status(b), Some(TaskStatus::Skipped));
    assert_eq!(sim.core.awaited_keys(), 0);
    // A later task with the key does not resurrect anything.
    let ghost = sim.submit(job(1.0).key("ghost-a"));
    sim.run_to_completion(10.0);
    assert_eq!(sim.status(ghost), Some(TaskStatus::Succeeded));
}

// --- v0.2: reservation conflicts are detected before shedding ---------------------------------

#[test]
fn reservation_conflict_does_not_shed_first() {
    let mut p = PoolConfig::fixed("default", 1);
    p.bounds.max_pending = Some(2);
    p.overflow = OverflowPolicy::ShedLowest;
    let mut sim = Sim::with_pool(p);
    sim.submit(job(100.0)); // running
    sim.submit(
        job(1.0)
            .duration_hint(secs(5.0))
            .fixed_start(FixedStart::at(t(50.0))),
    );
    let low = sim.submit(job(1.0).priority(Priority::Low));
    let r = sim.try_submit(
        job(1.0)
            .priority(Priority::High)
            .duration_hint(secs(5.0))
            .fixed_start(FixedStart::at(t(51.0))),
    );
    assert_eq!(r.unwrap_err(), SubmitError::ReservationConflict);
    assert_eq!(
        sim.status(low),
        Some(TaskStatus::Ready),
        "nothing shed for a rejected submission"
    );
}

// --- v0.2: recurring overlap guard survives recovery -----------------------------------------

#[test]
fn recovered_recurring_keeps_overlap_guard() {
    let mut sim = Sim::with_pool(PoolConfig::fixed("default", 4));
    sim.now = t(100.0);
    let spec = RecurringSpec {
        name: "slow".into(),
        cron: "*/10 * * * * *".into(),
        tz: None,
        template: job(25.0),
        overlap: OverlapPolicy::Skip,
        tolerance: secs(1.0),
        on_miss: MissPolicy::RunLate,
    };
    // The 100s occurrence was materialized and is running when the leader dies.
    let mut running = job(25.0).fixed_start(FixedStart {
        at: t(100.0),
        tolerance: secs(1.0),
        on_miss: MissPolicy::RunLate,
    });
    running
        .tags
        .push(("cronoq.recurring".into(), "slow".into()));
    sim.core.recover(
        sim.now,
        Recovered {
            tasks: vec![rec_task(7, 7, running)],
            terminal: vec![],
            recurring: vec![(spec, Some(t(110.0)))],
            next_id: 8,
        },
    );
    sim.drain();
    sim.run_until(t(135.0));
    // 110 and 120 overlap the recovered occurrence (100..125) and are skipped; 130 runs.
    let starts: Vec<f64> = sim.started.iter().map(|s| s.at.as_secs_f64()).collect();
    assert_eq!(starts, vec![100.0, 130.0], "{starts:?}");
}

// --- v0.2: hold lead adapts to statistics learned after submission ----------------------------

#[test]
fn hold_lead_uses_current_statistics() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.default_duration = secs(1.0);
    let mut sim = Sim::with_pool(pool);
    // Submitted before anything is known about durations (lead would be ~1s).
    let f = sim.submit(job(1.0).fixed_start(FixedStart::at(t(100.0))));
    // Then 4.4s tasks keep the single worker busy; the pool learns p90 ~= 4.4s. The worker frees
    // up at 96.8s: with a stale 1s lead the next 4.4s task takes it and runs past 100s.
    for _ in 0..40 {
        sim.submit(job(4.4));
    }
    sim.run_to_completion(300.0);
    let start = sim.start_of(f).unwrap();
    assert!(
        start.diff_secs(t(100.0)).abs() <= 1.0,
        "fixed task started at {start:?}"
    );
}

// --- Engine time as Unix nanoseconds ---------------------------------------------------------

#[test]
fn engine_works_with_unix_epoch_time() {
    let base = Time(1_800_000_000_000_000_000); // ~2027 in Unix nanoseconds
    let mut pool = PoolConfig::new("default").workers(1, 1, 8);
    pool.autoscale.as_mut().unwrap().interval = secs(1.0);
    let mut core: Core<u32> = Core::new(CoreConfig {
        pools: vec![pool],
        retention: secs(60.0),
        ..Default::default()
    });
    // The first call must not trigger a storm of catch-up autoscale evaluations.
    core.tick(base);
    let mut scale_actions = 0;
    while let Some(a) = core.poll_action() {
        scale_actions += matches!(a, Action::ScaleTo { .. }) as usize;
    }
    assert!(scale_actions <= 1);
    assert!(core.next_wakeup().unwrap() <= base + secs(1.0));
    for i in 0..50 {
        core.submit(base, TaskSpec::new(i)).unwrap();
    }
    let mut now = base;
    for _ in 0..200 {
        now = now + secs(0.5);
        let mut leases = vec![];
        while let Some(a) = core.poll_action() {
            match a {
                Action::Dispatch(d) => leases.push(d.lease),
                Action::ScaleTo { pool, desired } => core.set_capacity(now, pool, desired),
                _ => {}
            }
        }
        for l in leases {
            core.complete(now, l, Outcome::Success);
        }
        core.tick(now);
        core.check_invariants();
    }
    let s = core.snapshot(now);
    assert_eq!(s.totals.succeeded, 50);
    assert!(s.pools[0].utilization.is_finite() && s.pools[0].utilization <= 1.0);
    // Accounting starts at the first observed time, not at Time::ZERO (~57 years earlier).
    assert!(
        s.pools[0].worker_seconds < 8.0 * 101.0,
        "worker_seconds={}",
        s.pools[0].worker_seconds
    );
    assert!(
        s.pools[0].capacity > 1,
        "autoscaled: {}",
        s.pools[0].capacity
    );
    // Retention still evicts by age.
    core.tick(now + secs(3600.0));
    assert_eq!(core.status(&TaskRef::Id(TaskId(1 << 0 | 1))), None);
}
