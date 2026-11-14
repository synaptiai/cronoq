//! Engine time after a sleep: `resync_clock` and `advance_clock`.
//!
//! A sleeping machine cannot be had in a test. What a sleep does to the scheduler is that
//! the wall clock is ahead of engine time afterwards; `advance_clock` produces the same
//! state of the timers, and `resync_clock` is checked for what it does when nothing is behind.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use cronoq::{Pool, RetryPolicy, Scheduler, Task, TaskError};

const HOUR: Duration = Duration::from_secs(3600);

/// A pool whose executor fails every first attempt and counts the attempts.
fn flaky(attempts: Arc<AtomicU32>) -> Pool<u32, u32> {
    Pool::fixed("default", 2).executor_fn(move |ctx, x: Arc<u32>| {
        let attempts = attempts.clone();
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            if ctx.attempt == 1 {
                Err(TaskError::retryable("first attempt"))
            } else {
                Ok(*x)
            }
        }
    })
}

fn hourly_backoff() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_backoff: HOUR,
        multiplier: 1.0,
        max_backoff: HOUR,
        jitter: false,
    }
}

async fn eventually(what: &str, ok: impl Fn() -> bool) {
    let start = std::time::Instant::now();
    while !ok() {
        assert!(start.elapsed() < Duration::from_secs(5), "{what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn timers_that_became_due_during_a_gap_fire_at_once() {
    let attempts = Arc::new(AtomicU32::new(0));
    let s = Scheduler::builder()
        .pool(flaky(attempts.clone()))
        .build()
        .await
        .unwrap();
    // Three tasks wait out a backoff of an hour; a fourth starts in two hours.
    let mut retried = Vec::new();
    for i in 0..3 {
        retried.push(
            s.submit(Task::new(i).retry(hourly_backoff()))
                .await
                .unwrap(),
        );
    }
    let later = s
        .submit(Task::new(9).start_in(2 * HOUR).no_retry())
        .await
        .unwrap();
    eventually("first attempts", || attempts.load(Ordering::SeqCst) == 3).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "nothing is retried early"
    );

    // The machine "slept" for an hour and a minute: the backoffs are over, all at once.
    let before = s.clock().now();
    s.advance_clock(HOUR + Duration::from_secs(60))
        .await
        .unwrap();
    assert!(s.clock().now().0 - before.0 >= 3_660_000_000_000);
    for (i, h) in retried.into_iter().enumerate() {
        let out = tokio::time::timeout(Duration::from_secs(5), h).await;
        assert_eq!(*out.expect("retried promptly").unwrap(), i as u32);
    }
    // The task with the later start is not due yet.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 6);
    s.advance_clock(HOUR).await.unwrap();
    let out = tokio::time::timeout(Duration::from_secs(5), later).await;
    assert!(
        out.expect("started promptly").is_err(),
        "its only attempt fails"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 7);
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resync_without_a_gap_changes_nothing() {
    let attempts = Arc::new(AtomicU32::new(0));
    let s = Scheduler::builder()
        .pool(flaky(attempts.clone()))
        .build()
        .await
        .unwrap();
    let h = s
        .submit(Task::new(1).retry(hourly_backoff()))
        .await
        .unwrap();
    eventually("first attempt", || attempts.load(Ordering::SeqCst) == 1).await;
    // Engine time has followed the wall clock: there is at most scheduling noise to make up.
    let moved = s.resync_clock().await.unwrap();
    assert!(moved < Duration::from_secs(1), "{moved:?}");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "the backoff still runs");
    // Engine time is at the wall clock, give or take the noise.
    let wall = s.clock().from_system(std::time::SystemTime::now());
    assert!(s.clock().now().0.abs_diff(wall.0) < 1_000_000_000);
    s.advance_clock(HOUR).await.unwrap();
    // Ahead of the wall clock now (as after the wall clock was set back): no move back.
    let at = s.clock().now();
    assert_eq!(s.resync_clock().await.unwrap(), Duration::ZERO);
    assert!(s.clock().now() >= at);
    assert_eq!(
        *tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .unwrap()
            .unwrap(),
        1
    );
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_attempt_that_ran_across_the_gap_times_out() {
    let s = Scheduler::builder()
        .pool(
            Pool::<u32, u32>::fixed("default", 1).executor_fn(|_ctx, _x| async move {
                std::future::pending::<Result<u32, TaskError>>().await
            }),
        )
        .build()
        .await
        .unwrap();
    let h = s
        .submit(Task::new(1).timeout(Duration::from_secs(600)).no_retry())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    s.advance_clock(HOUR).await.unwrap();
    let out = tokio::time::timeout(Duration::from_secs(5), h).await;
    assert!(out.expect("timed out promptly").is_err());
    assert_eq!(s.snapshot().await.unwrap().totals.timeouts, 1);
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}
