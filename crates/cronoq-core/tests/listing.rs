//! Listing live tasks and recurring schedules.

mod common;

use common::*;
use cronoq_core::*;

fn sim() -> Sim {
    Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("a", 1), PoolConfig::fixed("b", 1)],
        ..Default::default()
    })
}

fn ids(v: Vec<TaskSummary>) -> Vec<u64> {
    v.into_iter().map(|t| t.id.0).collect()
}

#[test]
fn live_tasks_are_listed_by_pool_status_and_page() {
    let mut s = sim();
    let running = s.submit(TaskSpec::new(Job::ok(10.0)).pool("a").key("first"));
    let queued = s.submit(
        TaskSpec::new(Job::ok(10.0))
            .pool("a")
            .kind("llm")
            .priority(Priority::High),
    );
    s.submit(TaskSpec::new(Job::ok(10.0)).pool("b"));
    let done = s.submit(TaskSpec::new(Job::ok(0.0)).pool("b"));
    s.run_until(t(10.5));
    // The first task of each pool finished at 10s; `queued` and `done` started then.
    let late = s.submit(TaskSpec::new(Job::ok(10.0)).pool("a"));
    assert_eq!(s.status(running), Some(TaskStatus::Succeeded));
    assert_eq!(s.status(done), Some(TaskStatus::Succeeded));

    let none: [&str; 0] = [];
    let all = s.core.list_tasks(&none, &[], None, 100);
    let first = &all[0];
    assert_eq!(first.id, queued);
    assert_eq!(first.pool.as_ref(), "a");
    assert_eq!(first.kind.as_ref(), "llm");
    assert_eq!(first.priority, Priority::High);
    assert_eq!(first.status, TaskStatus::Running);
    assert_eq!(first.attempt, 1);
    // Finished tasks are not listed.
    assert_eq!(ids(all), [queued.0, late.0]);
    assert_eq!(
        ids(s.core.list_tasks(&["a"], &[TaskStatus::Ready], None, 100)),
        [late.0]
    );
    assert_eq!(
        ids(s.core.list_tasks(&["a"], &[], Some(queued), 100)),
        [late.0]
    );
    assert_eq!(
        ids(s.core.list_tasks(&["a", "b"], &[], None, 1)),
        [queued.0]
    );
    assert!(s.core.list_tasks(&["nope"], &[], None, 100).is_empty());
}

#[test]
fn recurring_schedules_are_listed() {
    let mut s = sim();
    let spec = RecurringSpec {
        name: "nightly".into(),
        cron: "0 * * * * *".into(),
        tz: None,
        template: TaskSpec::new(Job::ok(1.0)).pool("b").kind("report"),
        deadline_after: None,
        overlap: OverlapPolicy::Queue,
        tolerance: secs(1.0),
        on_miss: MissPolicy::RunLate,
    };
    s.core.add_recurring(s.now, spec).unwrap();
    s.run_until(t(61.0));
    let list = s.core.list_recurring();
    assert_eq!(list.len(), 1);
    let r = &list[0];
    assert_eq!(r.name.as_ref(), "nightly");
    assert_eq!(r.pool.as_ref(), "b");
    assert_eq!(r.kind.as_ref(), "report");
    assert_eq!(r.overlap, OverlapPolicy::Queue);
    assert_eq!(r.next, Some(t(120.0)));
    assert_eq!(r.last_task, Some(s.started[0].id));
}

#[test]
fn sharded_engines_issue_ids_in_their_shard() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("a", 1)],
        id_epoch: 7,
        id_shard: Some(3),
        ..Default::default()
    });
    let a = s.submit(TaskSpec::new(Job::ok(1.0)).pool("a"));
    let b = s.submit(TaskSpec::new(Job::ok(1.0)).pool("a"));
    assert_eq!(a, TaskId::first_in_shard(3, 7));
    assert_eq!((a.shard(), b.shard(), b.0 - a.0), (3, 3, 1));
    // The sequence carries into the epoch bits, not the shard's.
    let last_of_epoch = TaskId((3u64 << 56) | (7u64 << 40) | ((1u64 << 40) - 1));
    assert_eq!(TaskId(last_of_epoch.0 + 1).shard(), 3);
    // Recovered state of another shard is recognised, and so are unsharded ids whose top byte
    // is not the shard's; unsharded ids of early epochs look like shard 0's.
    let mut rec = Recovered::<Job> {
        next_id: b.0 + 1,
        ..Default::default()
    };
    assert_eq!(rec.outside_shard(3), None);
    assert_eq!(rec.outside_shard(4), Some(b));
    rec.next_id = (7u64 << 48) | 9;
    assert_eq!(rec.outside_shard(3), Some(TaskId((7u64 << 48) | 8)));
    assert_eq!(Recovered::<Job>::default().outside_shard(3), None);
}
