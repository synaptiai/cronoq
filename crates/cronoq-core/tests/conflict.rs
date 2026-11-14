//! `KeyConflict::Reject`: a duplicate key with a different fingerprint is an error, not a
//! silent duplicate.

mod common;

use common::*;
use cronoq_core::*;

fn one_worker() -> Sim {
    Sim::with_pool(PoolConfig::fixed("default", 1))
}

fn keyed(d: f64, fp: u128) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d)).key("k").fingerprint(fp)
}

fn reject() -> SubmitOptions {
    SubmitOptions {
        on_key_conflict: KeyConflict::Reject,
        ..Default::default()
    }
}

fn submit(s: &mut Sim, spec: TaskSpec<Job>, opts: SubmitOptions) -> Result<Submitted, SubmitError> {
    let r = s.core.submit_with(s.now, spec, opts);
    s.drain();
    r
}

#[test]
fn the_same_fingerprint_is_a_duplicate_and_another_is_a_conflict() {
    let mut s = one_worker();
    s.submit(TaskSpec::new(Job::ok(10.0)));
    let first = submit(&mut s, keyed(1.0, 7), reject()).unwrap();
    assert_eq!(first.outcome, SubmitOutcome::Created);
    // A retry of the same submission.
    let again = submit(&mut s, keyed(1.0, 7), reject()).unwrap();
    assert_eq!(
        (again.id, again.duplicate, again.outcome),
        (first.id, true, SubmitOutcome::Existing)
    );
    // The same key asking for something else.
    assert_eq!(
        submit(&mut s, keyed(2.0, 8), reject()),
        Err(SubmitError::KeyConflict {
            id: first.id,
            status: TaskStatus::Ready
        })
    );
    // The task is unchanged and runs what it was submitted with.
    s.run_to_completion(100.0);
    let run = s.started.iter().find(|x| x.id == first.id).unwrap();
    assert_eq!(run.dur, 1.0);
    assert_eq!(s.core.totals().submitted, 2);
}

#[test]
fn without_reject_a_different_fingerprint_is_still_a_silent_duplicate() {
    let mut s = one_worker();
    let first = submit(&mut s, keyed(10.0, 7), SubmitOptions::default()).unwrap();
    let other = submit(&mut s, keyed(2.0, 8), SubmitOptions::default()).unwrap();
    assert_eq!(
        (other.id, other.outcome),
        (first.id, SubmitOutcome::Existing)
    );
    let explicit = SubmitOptions {
        on_key_conflict: KeyConflict::ReturnExisting,
        ..Default::default()
    };
    assert_eq!(
        submit(&mut s, keyed(2.0, 8), explicit).unwrap().id,
        first.id
    );
}

#[test]
fn the_configured_default_applies_to_submissions_that_do_not_choose() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        on_key_conflict: KeyConflict::Reject,
        ..Default::default()
    });
    let first = s.submit(keyed(10.0, 7));
    assert!(matches!(
        s.try_submit(keyed(2.0, 8)),
        Err(SubmitError::KeyConflict { id, .. }) if id == first
    ));
    // A submission can still opt out.
    let explicit = SubmitOptions {
        on_key_conflict: KeyConflict::ReturnExisting,
        ..Default::default()
    };
    assert_eq!(submit(&mut s, keyed(2.0, 8), explicit).unwrap().id, first);
    // Replacing by key is a choice per submission, not a default.
    let cfg = CoreConfig {
        on_key_conflict: KeyConflict::ReplaceQueued,
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn a_missing_fingerprint_on_either_side_matches_anything() {
    let mut s = one_worker();
    s.submit(TaskSpec::new(Job::ok(10.0)));
    // The task has none (submitted before fingerprints, or without one).
    let plain = s.submit(TaskSpec::new(Job::ok(1.0)).key("plain"));
    let r = submit(
        &mut s,
        TaskSpec::new(Job::ok(2.0)).key("plain").fingerprint(9),
        reject(),
    )
    .unwrap();
    assert_eq!((r.id, r.outcome), (plain, SubmitOutcome::Existing));
    // The submission has none.
    let first = s.submit(keyed(1.0, 7));
    let r = submit(&mut s, TaskSpec::new(Job::ok(2.0)).key("k"), reject()).unwrap();
    assert_eq!(r.id, first);
}

#[test]
fn a_finished_task_still_answers_for_its_key_while_it_is_retained() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        retention: secs(100.0),
        ..Default::default()
    });
    let first = s.submit(keyed(1.0, 7));
    s.run_to_completion(10.0);
    assert_eq!(s.status(first), Some(TaskStatus::Succeeded));
    assert_eq!(submit(&mut s, keyed(1.0, 7), reject()).unwrap().id, first);
    assert_eq!(
        submit(&mut s, keyed(2.0, 8), reject()),
        Err(SubmitError::KeyConflict {
            id: first,
            status: TaskStatus::Succeeded
        })
    );
    // When retention ends, the fingerprint goes with the key: a new task is created.
    s.run_until(t(200.0));
    let _ = s.core.snapshot(s.now);
    let later = submit(&mut s, keyed(2.0, 8), reject()).unwrap();
    assert_eq!(later.outcome, SubmitOutcome::Created);
    assert_ne!(later.id, first);
}

#[test]
fn an_update_keeps_the_fingerprint_and_a_replacement_stores_the_new_one() {
    let mut s = one_worker();
    s.submit(TaskSpec::new(Job::ok(10.0)));
    let id = s.submit(keyed(1.0, 7));
    // Changing the priority does not turn a retry of the original into a conflict.
    s.update(id, TaskPatch::default().priority(Priority::High))
        .unwrap();
    assert_eq!(submit(&mut s, keyed(1.0, 7), reject()).unwrap().id, id);
    // A replacement is a new submission: its fingerprint is the task's from then on.
    s.replace(keyed(3.0, 8), false).unwrap();
    assert_eq!(submit(&mut s, keyed(3.0, 8), reject()).unwrap().id, id);
    assert!(matches!(
        submit(&mut s, keyed(1.0, 7), reject()),
        Err(SubmitError::KeyConflict { .. })
    ));
}

#[test]
fn the_newest_accepted_submission_of_a_key_is_the_one_compared() {
    let mut s = one_worker();
    // The task runs with fingerprint 7; a replacement with fingerprint 8 is stored behind it.
    let running = s.submit(keyed(10.0, 7));
    assert_eq!(
        s.replace(keyed(2.0, 8), false).unwrap().outcome,
        SubmitOutcome::Pending
    );
    // A retry of that replacement is a duplicate; the superseded submission is a conflict.
    assert_eq!(submit(&mut s, keyed(2.0, 8), reject()).unwrap().id, running);
    assert!(matches!(
        submit(&mut s, keyed(10.0, 7), reject()),
        Err(SubmitError::KeyConflict { id, .. }) if id == running
    ));
    // The same answers once the task has ended and the successor holds the key.
    s.run_until(t(11.0));
    let successor = submit(&mut s, keyed(2.0, 8), reject()).unwrap().id;
    assert_ne!(successor, running);
    assert!(matches!(
        submit(&mut s, keyed(10.0, 7), reject()),
        Err(SubmitError::KeyConflict { id, .. }) if id == successor
    ));
}

#[test]
fn retention_by_count_takes_the_fingerprint_with_the_key() {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        retention_max: 1,
        ..Default::default()
    });
    let old = s.submit(TaskSpec::new(Job::ok(1.0)).key("old").fingerprint(1));
    let kept = s.submit(TaskSpec::new(Job::ok(1.0)).key("kept").fingerprint(2));
    s.run_to_completion(10.0);
    let _ = s.core.snapshot(s.now);
    // Only the newest finished task is remembered.
    let other = |key: &str| TaskSpec::new(Job::ok(1.0)).key(key).fingerprint(9);
    assert!(matches!(
        submit(&mut s, other("kept"), reject()),
        Err(SubmitError::KeyConflict { id, .. }) if id == kept
    ));
    let fresh = submit(&mut s, other("old"), reject()).unwrap();
    assert_eq!(fresh.outcome, SubmitOutcome::Created);
    assert_ne!(fresh.id, old);
}

#[test]
fn a_batch_with_a_conflict_admits_nothing_and_names_the_index() {
    let mut s = one_worker();
    let first = s.submit(keyed(10.0, 7));
    let batch = || {
        vec![
            TaskSpec::new(Job::ok(1.0)).key("new-a").fingerprint(1),
            keyed(10.0, 7),
            keyed(2.0, 8),
            TaskSpec::new(Job::ok(1.0)).key("new-b").fingerprint(2),
        ]
    };
    let now = s.now;
    let r = s
        .core
        .submit_batch_with(now, batch(), &[KeyConflict::Reject; 4]);
    s.drain();
    assert_eq!(
        r,
        Err((
            2,
            SubmitError::KeyConflict {
                id: first,
                status: TaskStatus::Running
            }
        ))
    );
    assert_eq!(s.core.live_tasks(), 1, "nothing was admitted");
    assert_eq!(s.core.totals().submitted, 1);
    // Only the tasks that ask are compared: here the conflicting one does not.
    let mixed = [
        KeyConflict::Reject,
        KeyConflict::Reject,
        KeyConflict::ReturnExisting,
        KeyConflict::Reject,
    ];
    assert!(s.core.submit_batch_with(now, batch(), &mixed).is_ok());
    s.drain();
    // Without reject the same batch goes in, with duplicates for the existing key.
    let r = s.core.submit_batch(now, batch()).unwrap();
    s.drain();
    let outcomes: Vec<SubmitOutcome> = r.iter().map(|x| x.outcome).collect();
    assert_eq!(outcomes, [SubmitOutcome::Existing; 4]);
    // Replacing by key is not for batches.
    assert!(matches!(
        s.core
            .submit_batch_with(now, batch(), &[KeyConflict::ReplaceQueued; 4]),
        Err((0, SubmitError::Invalid(_)))
    ));
}

#[test]
fn conflicts_are_still_detected_after_recovery() {
    let cfg = || CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        persist: true,
        ..Default::default()
    };
    let mut s = Sim::new(cfg());
    let live = s.submit(TaskSpec::new(Job::ok(10.0)).key("live").fingerprint(1));
    let Some(Mutation::Submitted { spec, .. }) = s.persisted.first() else {
        panic!("persisted");
    };
    assert_eq!(
        spec.fingerprint,
        Some(1),
        "the fingerprint is persisted with the task"
    );

    let mut r = Sim::new(cfg());
    r.core.recover(
        r.now,
        Recovered {
            tasks: vec![RecoveredTask {
                id: live,
                seq: 1,
                arrival: Time::ZERO,
                spec: spec.clone(),
                attempt: 0,
                releases: 0,
            }],
            terminal: vec![RecoveredTerminal {
                id: TaskId(500),
                key: Some("done".into()),
                status: TaskStatus::Succeeded,
                at: Time::ZERO,
                fingerprint: Some(2),
            }],
            ..Default::default()
        },
    );
    r.drain();
    let other = |key: &str, fp| TaskSpec::new(Job::ok(1.0)).key(key).fingerprint(fp);
    assert_eq!(submit(&mut r, other("live", 1), reject()).unwrap().id, live);
    assert!(matches!(
        submit(&mut r, other("live", 9), reject()),
        Err(SubmitError::KeyConflict { id, .. }) if id == live
    ));
    assert_eq!(
        submit(&mut r, other("done", 2), reject()).unwrap().id,
        TaskId(500)
    );
    assert_eq!(
        submit(&mut r, other("done", 9), reject()),
        Err(SubmitError::KeyConflict {
            id: TaskId(500),
            status: TaskStatus::Succeeded
        })
    );
}
