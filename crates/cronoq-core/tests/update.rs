//! `Core::update`: changing the priority, deadline or tags of a task that has not finished.

mod common;

use common::*;
use cronoq_core::*;

fn one_worker() -> Sim {
    Sim::with_pool(PoolConfig::fixed("default", 1))
}

fn job(d: f64) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d))
}

fn class(p: Priority) -> TaskPatch {
    TaskPatch::default().priority(p)
}

/// Order in which the tasks submitted after the first (blocking) one started.
fn after_blocker(s: &Sim) -> Vec<TaskId> {
    s.order()[1..].to_vec()
}

// ------------------------------------------------------------------------------------------
// Priority
// ------------------------------------------------------------------------------------------

#[test]
fn a_promoted_task_runs_before_lower_classes_and_in_submission_order_in_its_new_class() {
    let mut s = one_worker();
    s.submit(job(10.0));
    let low = s.submit(job(1.0).priority(Priority::Low));
    let n1 = s.submit(job(1.0));
    let h1 = s.submit(job(1.0).priority(Priority::High));
    let n2 = s.submit(job(1.0));
    let h2 = s.submit(job(1.0).priority(Priority::High));

    let r = s.update(low, class(Priority::High)).unwrap();
    assert_eq!(
        r,
        Updated {
            id: low,
            status: TaskStatus::Ready,
            deferred: false,
            changed: true
        }
    );
    s.run_to_completion(100.0);
    // Among the high tasks it goes first: it was submitted before both.
    assert_eq!(after_blocker(&s), vec![low, h1, h2, n1, n2]);
}

#[test]
fn a_demoted_task_goes_behind_higher_classes_and_still_ages() {
    let mut s = one_worker();
    s.submit(job(100.0));
    let demoted = s.submit(job(1.0).priority(Priority::High));
    let normal = s.submit(job(1.0));
    s.update(demoted, class(Priority::Low)).unwrap();
    // A normal task submitted much later: the demoted one has aged past it by then
    // (10 + 2 * 99 > 100 + 0), because its waiting time counts from submission.
    s.run_until(t(99.0));
    let late = s.submit(job(1.0));
    s.run_to_completion(100.0);
    assert_eq!(after_blocker(&s), vec![normal, demoted, late]);
}

#[test]
fn giving_the_same_values_changes_nothing() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    });
    s.core.set_emit_transitions(true);
    s.submit(job(10.0));
    let id = s.submit(job(1.0).priority(Priority::High).deadline(t(50.0)));
    let (events, persisted) = (s.events.len(), s.persisted.len());

    let same = TaskPatch::default()
        .priority(Priority::High)
        .deadline(t(50.0))
        .tags(Vec::new());
    let r = s.update(id, same).unwrap();
    assert!(!r.changed && !r.deferred);
    let r = s.update(id, TaskPatch::default()).unwrap();
    assert!(!r.changed);
    assert_eq!((s.events.len(), s.persisted.len()), (events, persisted));
}

#[test]
fn a_running_task_is_not_interrupted_and_its_retry_uses_the_new_class() {
    let mut s = one_worker();
    // Fails once after 5 s, then is retried.
    let flaky = s.submit(
        TaskSpec::new(Job::flaky(5.0, 1))
            .priority(Priority::Low)
            .retry(RetryPolicy {
                max_attempts: 3,
                initial_backoff: secs(1.0),
                jitter: false,
                ..Default::default()
            }),
    );
    let high = s.submit(job(20.0).priority(Priority::High));
    let normal = s.submit(job(1.0));
    assert_eq!(s.status(flaky), Some(TaskStatus::Running));

    let r = s.update(flaky, class(Priority::Critical)).unwrap();
    assert!(r.deferred && r.changed);
    assert_eq!(r.status, TaskStatus::Running);
    assert_eq!(s.running_count(), 1, "the attempt keeps running");
    assert!(s.aborts.is_empty());

    s.run_to_completion(200.0);
    // After its failure at 5 s the high task takes the slot; the retry, now critical, runs
    // next, ahead of the normal task that a low retry would have waited behind.
    let order: Vec<(TaskId, u32)> = s.started.iter().map(|x| (x.id, x.attempt)).collect();
    assert_eq!(order, vec![(flaky, 1), (high, 1), (flaky, 2), (normal, 1)]);
}

#[test]
fn a_task_waiting_for_a_retry_can_be_updated() {
    let mut s = one_worker();
    let flaky = s.submit(
        TaskSpec::new(Job::flaky(1.0, 1))
            .priority(Priority::Low)
            .retry(RetryPolicy {
                max_attempts: 3,
                initial_backoff: secs(10.0),
                jitter: false,
                ..Default::default()
            }),
    );
    s.run_until(t(2.0));
    assert_eq!(s.status(flaky), Some(TaskStatus::Retrying));
    let blocker = s.submit(job(20.0));
    let normal = s.submit(job(1.0));
    let r = s.update(flaky, class(Priority::High)).unwrap();
    assert_eq!((r.status, r.deferred), (TaskStatus::Retrying, false));
    s.run_to_completion(200.0);
    let order: Vec<(TaskId, u32)> = s.started.iter().map(|x| (x.id, x.attempt)).collect();
    assert_eq!(
        order,
        vec![(flaky, 1), (blocker, 1), (flaky, 2), (normal, 1)]
    );
}

#[test]
fn a_blocked_task_takes_the_new_class_when_its_dependency_ends() {
    let mut s = one_worker();
    let parent = s.submit(job(5.0).key("parent"));
    let child = s.submit(job(1.0).priority(Priority::Low).after("parent"));
    let normal = s.submit(job(1.0));
    assert_eq!(s.status(child), Some(TaskStatus::Blocked));
    s.update(child, class(Priority::High)).unwrap();
    s.run_to_completion(100.0);
    assert_eq!(s.order(), vec![parent, child, normal]);
}

#[test]
fn a_promotion_is_allowed_past_the_bound_of_its_new_class() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.bounds.max_pending_per_class[Priority::High.index()] = Some(1);
    pool.overflow = OverflowPolicy::Reject;
    let mut s = Sim::with_pool(pool);
    s.submit(job(10.0));
    s.submit(job(1.0).priority(Priority::High));
    let low = s.submit(job(1.0).priority(Priority::Low));
    assert_eq!(
        s.try_submit(job(1.0).priority(Priority::High)),
        Err(SubmitError::Overloaded)
    );
    assert!(s.update(low, class(Priority::High)).unwrap().changed);
    let snap = s.core.snapshot(s.now);
    assert_eq!(snap.pools[0].pending_by_class, [0, 2, 0, 0]);
}

// ------------------------------------------------------------------------------------------
// Deadline
// ------------------------------------------------------------------------------------------

#[test]
fn a_deadline_can_be_set_changed_and_removed() {
    let mut s = one_worker();
    s.submit(job(10.0));
    let a = s.submit(job(1.0).duration_hint(secs(1.0)));
    let b = s.submit(job(1.0).duration_hint(secs(1.0)));
    let c = s.submit(job(1.0).duration_hint(secs(1.0)).deadline(t(11.0)));

    // b gets a deadline that is already due at 10 s: it leads. c's deadline is removed, so it
    // falls back to its place by submission order, behind a.
    s.update(b, TaskPatch::default().deadline(t(10.5))).unwrap();
    s.update(c, TaskPatch::default().no_deadline()).unwrap();
    s.run_to_completion(100.0);
    assert_eq!(after_blocker(&s), vec![b, a, c]);
}

#[test]
fn a_later_deadline_gives_way_to_an_earlier_one() {
    let mut s = one_worker();
    s.submit(job(10.0));
    let a = s.submit(job(1.0).duration_hint(secs(1.0)).deadline(t(11.0)));
    let b = s.submit(job(1.0).duration_hint(secs(1.0)).deadline(t(12.0)));
    s.update(a, TaskPatch::default().deadline(t(30.0))).unwrap();
    s.run_to_completion(100.0);
    assert_eq!(after_blocker(&s), vec![b, a]);
}

// ------------------------------------------------------------------------------------------
// Tags
// ------------------------------------------------------------------------------------------

#[test]
fn tags_are_replaced_except_the_engines_own() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    });
    s.submit(job(10.0));
    let id = s.submit(job(1.0).tag("cronoq.note", "kept").tag("old", "1"));

    let reserved = TaskPatch::default().tags(vec![("cronoq.note".into(), "x".into())]);
    assert_eq!(
        s.update(id, reserved),
        Err(UpdateError::Invalid(
            "tags starting with `cronoq.` are reserved"
        ))
    );
    let patch = TaskPatch::default().tags(vec![("new".into(), "2".into())]);
    assert!(s.update(id, patch).unwrap().changed);
    let Some(Mutation::Updated { tags, .. }) = s.persisted.last() else {
        panic!("an update is persisted");
    };
    let stored: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (&**k, &**v)).collect();
    assert_eq!(stored, [("new", "2"), ("cronoq.note", "kept")]);

    s.run_to_completion(100.0);
    let started = s.started.iter().find(|x| x.id == id).unwrap();
    assert_eq!(
        started.tags,
        [
            ("new".to_string(), "2".to_string()),
            ("cronoq.note".to_string(), "kept".to_string())
        ]
    );
}

#[test]
fn the_same_tags_in_any_order_are_not_a_change() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    });
    s.submit(job(10.0));
    // As the server submits them: the caller's tags, then the engine's own.
    let id = s.submit(
        job(1.0)
            .tag("b", "2")
            .tag("a", "1")
            .tag("cronoq.content_type", "text/plain"),
    );
    let before = s.persisted.len();
    for tags in [[("a", "1"), ("b", "2")], [("b", "2"), ("a", "1")]] {
        let tags = tags
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect();
        let r = s.update(id, TaskPatch::default().tags(tags)).unwrap();
        assert!(!r.changed);
    }
    assert_eq!(s.persisted.len(), before, "nothing is written");
    let fewer = TaskPatch::default().tags(vec![("a".into(), "1".into())]);
    assert!(s.update(id, fewer).unwrap().changed);
}

#[test]
fn a_deadline_beyond_the_clock_is_refused() {
    let mut s = one_worker();
    s.submit(job(10.0));
    let id = s.submit(job(1.0).deadline(t(50.0)));
    assert!(matches!(
        s.update(id, TaskPatch::default().deadline(Time::MAX)),
        Err(UpdateError::Invalid(_))
    ));
    // A deadline that has already passed is accepted: the task is as urgent as it can be.
    let other = s.submit(job(1.0));
    assert!(
        s.update(other, TaskPatch::default().deadline(t(0.0)))
            .unwrap()
            .changed
    );
    s.run_to_completion(100.0);
    assert_eq!(after_blocker(&s), [other, id]);
}

#[test]
fn an_update_that_changes_nothing_still_dispatches_work_that_became_due() {
    for attempt in 0..3 {
        let mut s = one_worker();
        let flaky = s.submit(TaskSpec::new(Job::flaky(1.0, 1)).retry(RetryPolicy {
            max_attempts: 3,
            initial_backoff: secs(10.0),
            jitter: false,
            ..Default::default()
        }));
        s.run_until(t(2.0));
        assert_eq!(s.status(flaky), Some(TaskStatus::Retrying));
        // The first call after the retry became due is an update with nothing to do.
        s.now = t(20.0);
        match attempt {
            0 => assert_eq!(
                s.update(TaskId(u64::MAX), class(Priority::High)),
                Err(UpdateError::NotFound)
            ),
            1 => assert!(!s.update(flaky, class(Priority::Normal)).unwrap().changed),
            _ => {
                let now = s.now;
                assert!(!s.core.cancel(now, &TaskRef::Id(TaskId(u64::MAX))));
                s.drain();
            }
        }
        assert_eq!(
            s.status(flaky),
            Some(TaskStatus::Running),
            "case {attempt}: the retry starts without another call"
        );
    }
}

// ------------------------------------------------------------------------------------------
// Refusals
// ------------------------------------------------------------------------------------------

#[test]
fn unknown_finished_and_fixed_start_tasks_are_refused() {
    let mut s = one_worker();
    let done = s.submit(job(1.0).key("done"));
    s.run_to_completion(10.0);
    assert_eq!(s.status(done), Some(TaskStatus::Succeeded));
    assert_eq!(
        s.update(done, class(Priority::High)),
        Err(UpdateError::NotFound)
    );
    assert_eq!(
        s.core
            .update(s.now, &TaskRef::Key("done".into()), class(Priority::High)),
        Err(UpdateError::NotFound)
    );
    assert_eq!(
        s.update(TaskId(999_999), class(Priority::High)),
        Err(UpdateError::NotFound)
    );

    let fixed = s.submit(job(1.0).fixed_start(FixedStart::at(s.now + secs(60.0))));
    assert_eq!(
        s.update(fixed, class(Priority::High)),
        Err(UpdateError::FixedStart)
    );
    // Even a patch that would change nothing is refused for a fixed start.
    assert_eq!(
        s.update(fixed, TaskPatch::default()),
        Err(UpdateError::FixedStart)
    );
}

#[test]
fn a_task_can_be_updated_by_key() {
    let mut s = one_worker();
    s.submit(job(10.0));
    let id = s.submit(job(1.0).key("k").priority(Priority::Low));
    let r = s
        .core
        .update(s.now, &TaskRef::Key("k".into()), class(Priority::Critical))
        .unwrap();
    s.drain();
    assert_eq!(r.id, id);
}

// ------------------------------------------------------------------------------------------
// Other queues
// ------------------------------------------------------------------------------------------

#[test]
fn tasks_waiting_for_a_resource_or_a_label_keep_their_place_in_that_queue() {
    let mut pool = PoolConfig::fixed("default", 0);
    pool.max_workers = 4;
    let mut s = Sim::new(CoreConfig {
        pools: vec![pool],
        resources: vec![ResourceConfig::semaphore("gpu", 1)],
        ..Default::default()
    });
    s.auto_capacity = false;
    let pid = s.core.pool_id("default").unwrap();
    s.core.set_capacity(s.now, pid, 2);
    s.drain();

    let holder = s.submit(job(10.0).resource("gpu", 1));
    let plain = s.submit(job(10.0));
    // Both slots are busy. Queue two tasks for the semaphore and one for a label nobody has.
    let low = s.submit(job(1.0).resource("gpu", 1).priority(Priority::Low));
    let normal = s.submit(job(1.0).resource("gpu", 1));
    let labelled = s.submit(job(1.0).label("zone", "b").priority(Priority::Low));
    s.core.set_capacity(s.now, pid, 3);
    s.drain();
    // The free slot was offered: the semaphore tasks are parked, the labelled one waits.
    assert_eq!(s.order(), vec![holder, plain]);

    s.update(low, class(Priority::Critical)).unwrap();
    s.update(labelled, class(Priority::High)).unwrap();
    assert_eq!(
        s.order(),
        vec![holder, plain],
        "an update starts nothing by itself"
    );

    let zone: [(std::sync::Arc<str>, std::sync::Arc<str>); 1] = [("zone".into(), "b".into())];
    s.core.set_group_capacity(s.now, pid, &zone, 1);
    s.drain();
    s.run_to_completion(100.0);
    let order = s.order();
    assert_eq!(order[2], labelled);
    // A resource's waiters are served in the order they started waiting, whatever their
    // class: the normal task reached the semaphore first, and the promotion does not move the
    // other one ahead of it.
    assert_eq!(&order[3..], &[normal, low]);
}

#[test]
fn updates_in_a_fair_pool_keep_the_fair_queues_consistent() {
    let mut pool = PoolConfig::fixed("default", 1);
    pool.fairness = Some(Fairness::default());
    let mut s = Sim::with_pool(pool);
    s.submit(job(10.0).tenant("a"));
    let a1 = s.submit(job(1.0).tenant("a"));
    let a2 = s.submit(job(1.0).tenant("a"));
    let b1 = s.submit(job(1.0).tenant("b").priority(Priority::Low));
    let b2 = s.submit(job(1.0).tenant("b").deadline(t(500.0)));

    // Into another class, out of the deadline queue into the fair queue, and into the deadline
    // queue from the fair queue. `drain` checks the engine's invariants after each.
    s.update(b1, class(Priority::High)).unwrap();
    s.update(b2, TaskPatch::default().no_deadline()).unwrap();
    s.update(a2, TaskPatch::default().deadline(t(400.0)))
        .unwrap();
    s.update(a2, class(Priority::Low)).unwrap();
    s.run_to_completion(100.0);
    let order = after_blocker(&s);
    assert_eq!(order[0], b1, "the promoted task leads: {order:?}");
    assert_eq!(order.len(), 4);
    assert!(order.contains(&a1) && order.contains(&a2) && order.contains(&b2));
    assert_eq!(
        *order.last().unwrap(),
        a2,
        "the demoted task is last: {order:?}"
    );
}

#[test]
fn changing_class_back_and_forth_costs_the_tenant_nothing() {
    let run = |flips: usize| {
        let mut pool = PoolConfig::fixed("default", 1);
        pool.fairness = Some(Fairness::default());
        let mut s = Sim::with_pool(pool);
        s.submit(job(10.0).tenant("z"));
        let a1 = s.submit(job(1.0).tenant("a"));
        for _ in 0..flips {
            s.update(a1, class(Priority::High)).unwrap();
            s.update(a1, class(Priority::Normal)).unwrap();
        }
        let a2 = s.submit(job(1.0).tenant("a"));
        let b: Vec<TaskId> = (0..3).map(|_| s.submit(job(1.0).tenant("b"))).collect();
        s.run_to_completion(100.0);
        let order = after_blocker(&s);
        let pos = |id| order.iter().position(|x| *x == id).unwrap();
        (pos(a1), pos(a2), pos(b[0]), pos(b[1]), pos(b[2]))
    };
    let untouched = run(0);
    assert_eq!(run(1), untouched);
    assert_eq!(run(5), untouched);
    // Tenant a still alternates with tenant b.
    assert!(untouched.1 < untouched.4, "{untouched:?}");
}

// ------------------------------------------------------------------------------------------
// Events and durability
// ------------------------------------------------------------------------------------------

#[test]
fn an_update_emits_one_event_and_one_mutation_with_the_tasks_values() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    });
    s.core.set_emit_transitions(true);
    s.submit(job(10.0));
    let id = s.submit(job(1.0).key("k").tag("a", "1"));
    let (events, persisted) = (s.events.len(), s.persisted.len());

    s.run_until(t(3.0));
    s.update(id, class(Priority::High).deadline(t(60.0)))
        .unwrap();
    assert_eq!(s.events.len(), events + 1);
    let e = s.events.last().unwrap();
    assert_eq!(
        (e.id, e.kind, e.status, e.priority, e.at),
        (
            id,
            EventKind::Updated,
            TaskStatus::Ready,
            Priority::High,
            t(3.0)
        )
    );
    assert_eq!(e.key.as_deref(), Some("k"));

    assert_eq!(s.persisted.len(), persisted + 1);
    match s.persisted.last().unwrap() {
        Mutation::Updated {
            id: mid,
            priority,
            deadline,
            tags,
            at,
        } => {
            assert_eq!(
                (*mid, *priority, *deadline, *at),
                (id, Priority::High, Some(t(60.0)), t(3.0))
            );
            // The whole tag set is written, also when the update did not touch it.
            assert_eq!(tags.len(), 1);
        }
        other => panic!("unexpected mutation {other:?}"),
    }
    // Transition events carry the class too.
    s.run_to_completion(100.0);
    let done = s
        .events
        .iter()
        .rev()
        .find(|e| e.id == id && e.status == TaskStatus::Succeeded)
        .unwrap();
    assert_eq!(
        (done.kind, done.priority),
        (EventKind::Transition, Priority::High)
    );
}

#[test]
fn without_transition_events_an_update_emits_no_event() {
    let mut s = one_worker();
    s.submit(job(10.0));
    let id = s.submit(job(1.0));
    let before = s.events.len();
    s.update(id, class(Priority::High)).unwrap();
    assert_eq!(s.events.len(), before);
}

#[test]
fn an_updated_task_recovers_with_its_new_values() {
    let cfg = || CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    };
    let mut s = Sim::new(cfg());
    s.submit(job(10.0));
    let low = s.submit(job(1.0).priority(Priority::Low));
    let normal = s.submit(job(1.0));
    s.update(low, class(Priority::High)).unwrap();
    let late = s.submit(job(1.0).priority(Priority::Low));
    s.update(
        late,
        TaskPatch::default()
            .deadline(t(12.0))
            .tags(vec![("why".into(), "due".into())]),
    )
    .unwrap();

    // Rebuild the engine from what was persisted, as a store would hand it back.
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
            Mutation::Updated {
                id,
                priority,
                deadline,
                tags,
                ..
            } => {
                let t = tasks.iter_mut().find(|t| t.id == *id).unwrap();
                t.spec.priority = *priority;
                t.spec.deadline = *deadline;
                t.spec.tags = tags.to_vec();
            }
            _ => {}
        }
    }
    let mut r = Sim::new(cfg());
    r.core.recover(
        r.now,
        Recovered {
            tasks,
            ..Default::default()
        },
    );
    r.drain();
    r.run_to_completion(100.0);
    let order = r.order();
    let pos = |id| order.iter().position(|x| *x == id).unwrap();
    assert!(pos(low) < pos(normal), "{order:?}");
    assert!(
        pos(late) < pos(normal),
        "the deadline is restored: {order:?}"
    );
    let started = r.started.iter().find(|x| x.id == late).unwrap();
    assert_eq!(started.tags, [("why".to_string(), "due".to_string())]);
}
