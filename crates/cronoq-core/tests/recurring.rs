//! Recurring templates: per-occurrence keys and deadlines, and queued (chained) occurrences.

mod common;

use std::time::Duration;

use common::*;
use cronoq_core::*;

fn every_minute(job: Job, overlap: OverlapPolicy) -> RecurringSpec<Job> {
    RecurringSpec {
        name: "r".into(),
        cron: "0 * * * * *".into(),
        tz: None,
        template: TaskSpec::new(job),
        deadline_after: None,
        overlap,
        tolerance: secs(1.0),
        on_miss: MissPolicy::RunLate,
    }
}

fn sim() -> Sim {
    Sim::with_pool(PoolConfig::fixed("default", 4))
}

fn ended(s: &Sim, status: TaskStatus) -> usize {
    s.events.iter().filter(|e| e.status == status).count()
}

#[test]
fn occurrences_get_keys_and_relative_deadlines() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 4)],
        persist: true,
        ..Default::default()
    });
    let mut spec = every_minute(Job::ok(1.0), OverlapPolicy::Skip);
    spec.template.key = Some("nightly".into());
    spec.deadline_after = Some(secs(30.0));
    s.core.add_recurring(s.now, spec).unwrap();
    s.run_until(t(61.5));
    let id = s.started[0].id;
    let key = TaskRef::Key("nightly@1970-01-01T00:01:00Z".into());
    assert_eq!(s.core.status(&key), s.status(id));
    let deadline = s
        .persisted
        .iter()
        .find_map(|m| match m {
            Mutation::Submitted { id: i, spec, .. } if *i == id => Some(spec.deadline),
            _ => None,
        })
        .unwrap();
    assert_eq!(deadline, Some(t(90.0)));
}

// Occurrences of a queued schedule wait for the previous one instead of being skipped.
#[test]
fn queued_occurrences_run_one_after_another() {
    for (overlap, starts) in [
        (OverlapPolicy::Queue, vec![t(60.0), t(160.0), t(260.0)]),
        (OverlapPolicy::Skip, vec![t(60.0), t(180.0)]),
    ] {
        let mut s = sim();
        s.core
            .add_recurring(s.now, every_minute(Job::ok(100.0), overlap))
            .unwrap();
        s.run_until(t(270.0));
        let got: Vec<Time> = s.started.iter().map(|x| x.at).collect();
        assert_eq!(got, starts, "{overlap:?}");
        if overlap == OverlapPolicy::Queue {
            assert_eq!(ended(&s, TaskStatus::Skipped), 0);
        }
    }
}

// QueueOnSuccess: after a failure the following occurrences are skipped, until the schedule
// is added again.
#[test]
fn queue_on_success_stops_after_a_failure_until_re_added() {
    let mut s = sim();
    let spec = every_minute(Job::broken(1.0), OverlapPolicy::QueueOnSuccess);
    s.core.add_recurring(s.now, spec.clone()).unwrap();
    s.run_until(t(200.0));
    assert_eq!(s.started.len(), 1, "only the first occurrence ran");
    assert_eq!(ended(&s, TaskStatus::Failed), 1);
    assert_eq!(ended(&s, TaskStatus::Skipped), 2); // at 120 and 180
    s.core.add_recurring(s.now, spec).unwrap();
    s.run_until(t(250.0));
    assert_eq!(
        s.started.len(),
        2,
        "the re-added schedule starts a new chain"
    );
}

// The chain survives a restart: the occurrence after recovery still waits for the recorded
// predecessor.
#[test]
fn recovered_queued_schedule_waits_for_its_predecessor() {
    let mut s = sim();
    let prev = TaskId(7);
    let rec = Recovered {
        tasks: vec![RecoveredTask {
            id: prev,
            seq: 0,
            arrival: Time::ZERO,
            spec: TaskSpec::new(Job::ok(100.0)),
            attempt: 0,
        }],
        terminal: vec![],
        recurring: vec![RecoveredRecurring {
            spec: every_minute(Job::ok(1.0), OverlapPolicy::Queue),
            next: Some(t(60.0)),
            last: Some(prev),
        }],
        next_id: 8,
    };
    s.core.recover(s.now, rec);
    s.drain();
    s.run_until(t(150.0));
    let occurrence = s.started.iter().find(|x| x.id != prev).unwrap();
    assert_eq!(
        occurrence.at,
        t(100.0),
        "started when the predecessor finished"
    );
}

// A keyed occurrence that already exists (created before a failover) is not created again.
#[test]
fn keyed_occurrences_are_not_duplicated_after_recovery() {
    let mut s = sim();
    let mut spec = every_minute(Job::ok(1.0), OverlapPolicy::Allow);
    spec.template.key = Some("k".into());
    let existing = TaskSpec::new(Job::ok(1.0))
        .key("k@1970-01-01T00:01:00Z")
        .tag("cronoq.recurring", "r")
        .fixed_start(FixedStart {
            at: t(60.0),
            tolerance: Duration::from_secs(1),
            on_miss: MissPolicy::RunLate,
        });
    let rec = Recovered {
        tasks: vec![RecoveredTask {
            id: TaskId(3),
            seq: 0,
            arrival: Time::ZERO,
            spec: existing,
            attempt: 0,
        }],
        terminal: vec![],
        recurring: vec![RecoveredRecurring {
            spec,
            next: Some(t(60.0)),
            last: None,
        }],
        next_id: 4,
    };
    s.core.recover(s.now, rec);
    s.drain();
    s.run_until(t(61.5));
    assert_eq!(s.started.len(), 1);
    assert_eq!(s.started[0].id, TaskId(3));
}

// A task that is not the schedule's own occurrence (another pool, or untagged) cannot stand in
// for an occurrence by taking its key first: the occurrence runs without the key.
#[test]
fn a_foreign_task_holding_the_key_does_not_replace_the_occurrence() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![
            PoolConfig::fixed("default", 2),
            PoolConfig::fixed("other", 1),
        ],
        ..Default::default()
    });
    let squatter = s.submit(
        TaskSpec::new(Job::ok(1000.0))
            .pool("other")
            .key("q@1970-01-01T00:01:00Z"),
    );
    let mut spec = every_minute(Job::ok(1.0), OverlapPolicy::Queue);
    spec.template.key = Some("q".into());
    s.core.add_recurring(s.now, spec).unwrap();
    s.run_until(t(125.0));
    let runs: Vec<_> = s
        .started
        .iter()
        .filter(|x| x.id != squatter)
        .map(|x| x.at)
        .collect();
    assert_eq!(
        runs,
        vec![t(60.0), t(120.0)],
        "occurrences ran despite the squatter"
    );
}
