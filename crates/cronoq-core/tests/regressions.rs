//! Regression tests for bugs found in review (each failed before its fix).

mod common;
use common::*;
use cronoq_core::*;
use std::time::Duration;

fn job(d: f64) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d))
}

// T1: ShedLowest sheds every lower-class task even when the violated bound is the
// incoming class's own per-class bound, then rejects anyway.
#[test]
fn shed_does_not_evict_when_it_cannot_help() {
    let mut p = PoolConfig::fixed("default", 1);
    p.overflow = OverflowPolicy::ShedLowest;
    p.bounds.max_pending_per_class[Priority::High.index()] = Some(1);
    let mut s = Sim::with_pool(p);
    s.submit(job(100.0)); // occupies the worker
    let lows: Vec<_> = (0..5)
        .map(|_| s.submit(job(1.0).priority(Priority::Low)))
        .collect();
    s.submit(job(1.0).priority(Priority::High));
    let r = s.try_submit(job(1.0).priority(Priority::High));
    let _ = format!("second high: {:?}", r);
    let shed = lows
        .iter()
        .filter(|id| s.status(**id) == Some(TaskStatus::Shed))
        .count();
    let _ = format!("low tasks shed: {shed}");
    assert!(r.is_err());
    assert_eq!(shed, 0, "lows were shed for nothing");
}

// T2: cancelling the head parked waiter leaves later waiters parked though permits are free.
#[test]
fn cancelling_parked_task_wakes_next_waiter() {
    let cfg = CoreConfig {
        pools: vec![PoolConfig::fixed("default", 4)],
        resources: vec![ResourceConfig::semaphore("db", 2)],
        ..Default::default()
    };
    let mut s = Sim::new(cfg);
    let _b1 = s.submit(job(1.0).resource("db", 1));
    let _b2 = s.submit(job(100.0).resource("db", 1));
    let a = s.submit(job(1.0).resource("db", 2));
    let c = s.submit(job(1.0).resource("db", 1));
    s.run_until(t(2.0)); // b1 done -> 1 permit free; a (needs 2) blocks c
    let _ = format!("c status at 2s: {:?}", s.status(c));
    s.core.cancel(s.now, &TaskRef::Id(a));
    s.drain();
    s.run_until(t(50.0));
    let _ = format!(
        "c status at 50s: {:?}, started: {:?}",
        s.status(c),
        s.start_of(c)
    );
    assert!(
        s.start_of(c).is_some(),
        "c should have run with a free permit"
    );
}

// T3: cycle through an in-batch intermediate and an Await edge is not detected.
#[test]
fn batch_cycle_through_awaited_key_is_rejected() {
    let mut p = PoolConfig::fixed("default", 2);
    p.unknown_dependency = UnknownDependency::Await {
        timeout: Some(Duration::from_secs(5)),
    };
    let mut s = Sim::with_pool(p);
    let x = s.submit(job(1.0).key("X").after(TaskRef::Key("B".into())));
    let r = s.core.submit_batch(
        s.now,
        vec![
            job(1.0).key("A").after(TaskRef::Key("X".into())),
            job(1.0).key("B").after(TaskRef::Key("A".into())),
        ],
    );
    s.drain();
    let _ = format!("batch: {:?}", r.as_ref().map(|v| v.len()));
    s.run_until(t(1000.0));
    let _ = format!(
        "X {:?} A {:?} B {:?} live {}",
        s.status(x),
        s.core.status(&"A".into()),
        s.core.status(&"B".into()),
        s.core.live_tasks()
    );
    assert_eq!(r.unwrap_err().1, SubmitError::Cycle);
    // The awaiting task still times out instead of hanging.
    assert_eq!(s.status(x), Some(TaskStatus::Skipped));
}

// T4: recovering a recurring schedule whose stored `next` is far in the past materializes
// every missed occurrence in one go.
#[test]
fn recovered_recurring_does_not_replay_missed_occurrences() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 4));
    s.now = t(3600.0);
    let spec = RecurringSpec {
        name: "tick".into(),
        cron: "* * * * * *".into(),
        tz: None,
        template: job(0.1).kind("tick"),
        overlap: OverlapPolicy::Allow,
        tolerance: secs(1.0),
        on_miss: MissPolicy::Skip,
    };
    s.core.recover(
        s.now,
        Recovered {
            tasks: vec![],
            terminal: vec![],
            recurring: vec![(spec, Some(t(1.0)))],
            next_id: 1,
        },
    );
    s.drain();
    let submitted = s.core.totals().submitted;
    let _ = format!(
        "tasks materialized on recovery: {submitted}, missed {}",
        s.core.totals().missed
    );
    s.run_until(t(3601.0));
    assert!(submitted < 5, "{submitted}");
    assert!(
        s.core.totals().submitted < 5,
        "{}",
        s.core.totals().submitted
    );
}

// T5: a recovered fixed-start task (was running) ignores both its retry budget and on_miss=Skip.
#[test]
fn recovered_fixed_task_respects_budget_and_miss_policy() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 2));
    s.now = t(100.0);
    let spec = job(1.0)
        .fixed_start(FixedStart {
            at: t(10.0),
            tolerance: secs(1.0),
            on_miss: MissPolicy::Skip,
        })
        .retry(RetryPolicy {
            max_attempts: 1,
            ..Default::default()
        });
    s.core.recover(
        s.now,
        Recovered {
            tasks: vec![RecoveredTask {
                id: TaskId(1),
                seq: 0,
                arrival: t(0.0),
                spec,
                attempt: 1,
            }],
            terminal: vec![],
            recurring: vec![],
            next_id: 2,
        },
    );
    s.drain();
    s.run_until(t(110.0));
    let _ = format!(
        "started: {:?} status {:?}",
        s.started
            .iter()
            .map(|x| (x.at.as_secs_f64(), x.attempt))
            .collect::<Vec<_>>(),
        s.status(TaskId(1))
    );
    assert!(
        s.started.is_empty(),
        "should not re-run: budget exhausted and start missed"
    );
}

// T6: re-adding (upserting) a recurring schedule inside the materialization lead window
// materializes the same occurrence twice.
#[test]
fn re_adding_recurring_does_not_duplicate_occurrence() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 4));
    let spec = || RecurringSpec {
        name: "tick".into(),
        cron: "0 * * * * *".into(), // every minute at :00
        tz: None,
        template: job(1.0).kind("tick"),
        overlap: OverlapPolicy::Skip,
        tolerance: secs(5.0),
        on_miss: MissPolicy::RunLate,
    };
    s.core.add_recurring(s.now, spec()).unwrap();
    s.drain();
    s.run_until(t(58.0)); // occurrence at 60 already materialized (lead = 5s + 5s)
    s.core.add_recurring(s.now, spec()).unwrap(); // idempotent re-registration
    s.drain();
    s.run_until(t(70.0));
    let at60: Vec<_> = s
        .started
        .iter()
        .filter(|x| x.kind == "tick")
        .map(|x| x.at.as_secs_f64())
        .collect();
    let _ = format!(
        "tick starts: {at60:?}, submitted {}",
        s.core.totals().submitted
    );
    assert_eq!(at60.len(), 1);
}

// Batches respect global bounds summed across pools and classes.
#[test]
fn batch_respects_global_bounds() {
    let cfg = CoreConfig {
        pools: vec![PoolConfig::fixed("default", 0)],
        global_bounds: Bounds {
            max_pending: Some(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut s = Sim::new(cfg);
    let mut batch: Vec<_> = (0..6).map(|_| job(1.0)).collect();
    batch.extend((0..6).map(|_| job(1.0).priority(Priority::High)));
    assert!(s.core.submit_batch(s.now, batch).is_err());
    assert_eq!(s.core.live_tasks(), 0);
}

// Tasks whose pool no longer exists are closed out (not silently dropped) on recovery.
#[test]
fn recovery_closes_tasks_for_removed_pools() {
    let cfg = CoreConfig {
        persist: true,
        pools: vec![PoolConfig::fixed("default", 1)],
        ..Default::default()
    };
    let mut s = Sim::new(cfg);
    s.core.recover(
        s.now,
        Recovered {
            tasks: vec![RecoveredTask {
                id: TaskId(7),
                seq: 0,
                arrival: t(0.0),
                spec: job(1.0).pool("gone").key("k"),
                attempt: 0,
            }],
            terminal: vec![],
            recurring: vec![],
            next_id: 8,
        },
    );
    s.drain();
    assert_eq!(s.status(TaskId(7)), Some(TaskStatus::Failed));
    assert!(s.persisted.iter().any(|m| matches!(
        m,
        Mutation::Status {
            id: TaskId(7),
            status: TaskStatus::Failed,
            ..
        }
    )));
}

// Huge durations saturate instead of wrapping.
#[test]
fn huge_timeout_saturates() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 1));
    s.hang = true;
    let id = s.submit(job(1.0).timeout(Duration::MAX));
    s.run_until(t(100.0));
    assert_eq!(s.status(id), Some(TaskStatus::Running));
}

// --- Recurring templates are checked against the configuration -------------------------------
// An unknown resource in a template used to panic in `insert` when the occurrence fired, and the
// persisted schedule made every restart panic again.

fn gpu_sim() -> Sim {
    Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 2)],
        resources: vec![ResourceConfig::semaphore("gpu", 1)],
        ..Default::default()
    })
}

fn every_minute(template: TaskSpec<Job>) -> RecurringSpec<Job> {
    RecurringSpec {
        name: "r".into(),
        cron: "0 * * * * *".into(),
        tz: None,
        template,
        overlap: OverlapPolicy::Allow,
        tolerance: Duration::from_secs(1),
        on_miss: MissPolicy::Skip,
    }
}

#[test]
fn recurring_template_with_unknown_resource_is_rejected() {
    let mut s = gpu_sim();
    let spec = every_minute(TaskSpec::new(Job::ok(1.0)).resource("missing", 1));
    let err = s.core.add_recurring(s.now, spec).unwrap_err();
    assert!(err.contains("unknown resource 'missing'"), "{err}");
    s.run_until(t(130.0));
    assert!(s.started.is_empty());
}

#[test]
fn recurring_template_over_capacity_is_rejected() {
    let mut s = gpu_sim();
    let spec = every_minute(TaskSpec::new(Job::ok(1.0)).resource("gpu", 2));
    let err = s.core.add_recurring(s.now, spec).unwrap_err();
    assert!(err.contains("exceeds capacity"), "{err}");
}

#[test]
fn recurring_template_with_zero_attempts_is_rejected() {
    let mut s = gpu_sim();
    let template = TaskSpec::new(Job::ok(1.0)).retry(RetryPolicy {
        max_attempts: 0,
        ..Default::default()
    });
    assert!(s.core.add_recurring(s.now, every_minute(template)).is_err());
}

#[test]
fn recovered_schedule_with_removed_resource_is_not_armed() {
    let mut s = gpu_sim();
    let rec = Recovered {
        tasks: vec![],
        terminal: vec![],
        recurring: vec![(
            every_minute(TaskSpec::new(Job::ok(1.0)).resource("removed", 1)),
            None,
        )],
        next_id: 1,
    };
    s.core.recover(s.now, rec);
    s.run_until(t(130.0));
    assert!(s.started.is_empty());
    assert_eq!(s.core.totals().recurring_invalid, 1);
    assert!(s.core.recurring_names().is_empty());
}

#[test]
fn recovered_task_over_lowered_capacity_fails_instead_of_blocking() {
    let mut s = gpu_sim();
    let rec = Recovered {
        tasks: vec![RecoveredTask {
            id: TaskId(1),
            seq: 0,
            arrival: Time::ZERO,
            spec: TaskSpec::new(Job::ok(1.0)).resource("gpu", 2),
            attempt: 0,
        }],
        terminal: vec![],
        recurring: vec![],
        next_id: 2,
    };
    s.core.recover(s.now, rec);
    s.drain();
    assert_eq!(s.status(TaskId(1)), Some(TaskStatus::Failed));
}

// Lease ids restarted at 1 with every leader, so a report from an earlier leader's worker could
// name a lease the new leader had just issued. They carry the epoch now, like task ids.
#[test]
fn lease_ids_carry_the_epoch_and_stale_completions_are_counted() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        id_epoch: 3,
        ..Default::default()
    });
    let id = s.submit(TaskSpec::new(Job::ok(1.0)));
    assert_eq!(id.0 >> 48, 3);
    let lease = s.running_leases()[0];
    assert_eq!(lease.0 >> 48, 3);
    // A lease of the same number from epoch 2 is unknown here.
    let old = LeaseId((2 << 48) | (lease.0 & ((1 << 48) - 1)));
    assert!(!s.core.complete(s.now, old, Outcome::Success));
    assert_eq!(s.core.totals().stale_completions, 1);
    assert_eq!(s.status(id), Some(TaskStatus::Running));
    assert!(s.core.complete(s.now, lease, Outcome::Success));
    assert!(!s.core.complete(s.now, lease, Outcome::Success)); // duplicate
    assert_eq!(s.core.totals().stale_completions, 2);
    assert_eq!(s.status(id), Some(TaskStatus::Succeeded));
}
