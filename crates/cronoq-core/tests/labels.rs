//! Worker labels: tasks that require labels run only in matching worker groups.

mod common;

use std::sync::Arc;

use common::*;
use cronoq_core::*;

fn gpu() -> Vec<(Arc<str>, Arc<str>)> {
    vec![("gpu".into(), "a100".into())]
}

/// A pool with `plain` unlabeled slots and `gpus` slots labeled gpu=a100.
fn sim(plain: u32, gpus: u32) -> Sim {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", plain));
    s.core.set_group_capacity(s.now, PoolId(0), &gpu(), gpus);
    s.drain();
    s
}

fn group_of(s: &Sim, id: TaskId) -> Vec<(Arc<str>, Arc<str>)> {
    s.dispatched_group(id).expect("dispatched")
}

#[test]
fn tasks_needing_labels_run_only_in_matching_groups() {
    let mut s = sim(1, 1);
    let needs_gpu = s.submit(TaskSpec::new(Job::ok(10.0)).label("gpu", "a100"));
    let plain = s.submit(TaskSpec::new(Job::ok(10.0)));
    let waits = s.submit(TaskSpec::new(Job::ok(10.0)).label("gpu", "a100"));
    assert_eq!(group_of(&s, needs_gpu), gpu());
    assert_eq!(group_of(&s, plain), vec![]);
    assert_eq!(s.status(waits), Some(TaskStatus::Ready));
    // The waiting task takes the GPU slot as soon as it frees.
    s.run_until(t(10.5));
    assert_eq!(s.start_of(waits), Some(t(10.0)));
    assert_eq!(group_of(&s, waits), gpu());
}

#[test]
fn a_task_waiting_for_labels_does_not_block_the_pool() {
    let mut s = sim(2, 0);
    let blocked = s.submit(
        TaskSpec::new(Job::ok(1.0))
            .label("gpu", "a100")
            .priority(Priority::Critical),
    );
    let a = s.submit(TaskSpec::new(Job::ok(1.0)));
    let b = s.submit(TaskSpec::new(Job::ok(1.0)));
    assert_eq!(s.start_of(a), Some(t(0.0)));
    assert_eq!(s.start_of(b), Some(t(0.0)));
    assert_eq!(s.status(blocked), Some(TaskStatus::Ready));
    // A matching worker connects: the waiting task starts.
    s.core.set_group_capacity(s.now, PoolId(0), &gpu(), 1);
    s.drain();
    assert_eq!(s.start_of(blocked), Some(t(0.0)));
}

#[test]
fn unconstrained_work_prefers_the_least_specialized_group() {
    let mut s = sim(1, 1);
    let first = s.submit(TaskSpec::new(Job::ok(10.0)));
    assert_eq!(group_of(&s, first), vec![], "unlabeled slot used first");
    // With the unlabeled slot busy, plain work may still use the GPU slot.
    let second = s.submit(TaskSpec::new(Job::ok(10.0)));
    assert_eq!(group_of(&s, second), gpu());
}

#[test]
fn groups_with_more_labels_satisfy_fewer_requirements() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 0));
    let both: Vec<(Arc<str>, Arc<str>)> =
        vec![("gpu".into(), "a100".into()), ("zone".into(), "eu".into())];
    s.core.set_group_capacity(s.now, PoolId(0), &both, 1);
    s.drain();
    let eu_gpu = s.submit(
        TaskSpec::new(Job::ok(1.0))
            .label("zone", "eu")
            .label("gpu", "a100"),
    );
    let us = s.submit(TaskSpec::new(Job::ok(1.0)).label("zone", "us"));
    assert_eq!(s.start_of(eu_gpu), Some(t(0.0)));
    s.run_until(t(5.0));
    assert_eq!(s.start_of(us), None, "no group has zone=us");
}

#[test]
fn snapshot_reports_capacity_per_group() {
    let mut s = sim(2, 3);
    s.submit(TaskSpec::new(Job::ok(10.0)).label("gpu", "a100"));
    let snap = s.core.snapshot(s.now);
    let p = &snap.pools[0];
    assert_eq!(p.capacity, 5);
    assert_eq!(p.groups.len(), 2);
    assert_eq!((p.groups[0].capacity, p.groups[0].busy), (2, 0));
    assert_eq!(p.groups[1].labels, gpu());
    assert_eq!((p.groups[1].capacity, p.groups[1].busy), (3, 1));
}

#[test]
fn cancelling_a_task_waiting_for_labels_cleans_up() {
    let mut s = sim(1, 0);
    let w = s.submit(TaskSpec::new(Job::ok(1.0)).label("gpu", "a100"));
    assert!(s.core.cancel(s.now, &TaskRef::Id(w)));
    s.drain();
    s.core.set_group_capacity(s.now, PoolId(0), &gpu(), 1);
    s.drain();
    assert_eq!(s.start_of(w), None);
    assert_eq!(s.status(w), Some(TaskStatus::Cancelled));
}
