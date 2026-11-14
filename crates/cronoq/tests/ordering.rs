//! Ordering keys through the driver: tasks that share a key never overlap and run in
//! submission order, keys do not wait for each other, and a key can be halted and resumed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cronoq::*;
use tokio::time::Instant;

type Sched = Scheduler<(u64, u64), ()>;

/// What ran, in start order, and how many tasks of each key were running at once at most.
#[derive(Default)]
struct Seen {
    started: Vec<(u64, u64)>,
    running: HashMap<u64, u32>,
    overlap: u32,
    busy: u32,
    max_busy: u32,
}

fn pool(workers: u32, seen: Arc<Mutex<Seen>>, work: Duration) -> Pool<(u64, u64), ()> {
    Pool::fixed("default", workers).executor_fn(move |_ctx, x: Arc<(u64, u64)>| {
        let seen = seen.clone();
        async move {
            let (key, n) = *x;
            {
                let mut s = seen.lock().unwrap();
                s.started.push((key, n));
                let r = s.running.entry(key).or_default();
                *r += 1;
                let r = *r;
                s.overlap = s.overlap.max(r);
                s.busy += 1;
                s.max_busy = s.max_busy.max(s.busy);
            }
            // Key 0 is the slow one.
            tokio::time::sleep(if key == 0 { work * 50 } else { work }).await;
            let mut s = seen.lock().unwrap();
            *s.running.get_mut(&key).unwrap() -= 1;
            s.busy -= 1;
            if n == u64::MAX {
                return Err(TaskError::permanent("boom"));
            }
            Ok(())
        }
    })
}

fn task(key: u64, n: u64) -> Task<(u64, u64)> {
    Task::new((key, n)).ordering_key(format!("k{key}"))
}

#[tokio::test(start_paused = true)]
async fn tasks_of_a_key_never_overlap_and_keep_their_order() {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let s: Sched = Scheduler::builder()
        .pool(pool(8, seen.clone(), Duration::from_millis(10)))
        .build()
        .await
        .unwrap();
    // 1,000 keys with three tasks each, on eight workers, submitted round by round.
    let mut handles = Vec::new();
    for n in 0..3 {
        for key in 1..=1000 {
            handles.push(s.submit(task(key, n)).await.unwrap());
        }
    }
    let snap = s.snapshot().await.unwrap();
    assert_eq!(snap.pools[0].ordering.keys, 1000);
    for h in handles {
        h.await.unwrap();
    }
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.overlap, 1, "two tasks of one key ran at once");
        assert_eq!(seen.max_busy, 8, "up to eight keys run at once");
        let mut next: HashMap<u64, u64> = HashMap::new();
        for (key, n) in &seen.started {
            let want = next.entry(*key).or_default();
            assert_eq!(n, want, "key {key} started out of order");
            *want += 1;
        }
    }
    assert_eq!(s.snapshot().await.unwrap().pools[0].ordering.keys, 0);
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_slow_key_does_not_slow_the_others() {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let work = Duration::from_millis(100);
    let s: Sched = Scheduler::builder()
        .pool(pool(4, seen.clone(), work))
        .build()
        .await
        .unwrap();
    // Key 0 has ten slow tasks (five seconds each), submitted first. It may use one worker.
    for n in 0..10 {
        s.enqueue(task(0, n)).await.unwrap();
    }
    // 300 tasks of 100 other keys: 30 seconds of work for the three workers left, 10 seconds.
    let start = Instant::now();
    let mut handles = Vec::new();
    for n in 0..3 {
        for key in 1..=100 {
            handles.push(s.submit(task(key, n)).await.unwrap());
        }
    }
    for h in handles {
        h.await.unwrap();
    }
    let took = start.elapsed();
    assert!(
        took <= Duration::from_millis(10_500),
        "the other keys took {took:?}: the slow key held more than one worker"
    );
    // The slow key is still on its third task of ten, in order.
    let slow: Vec<u64> = seen
        .lock()
        .unwrap()
        .started
        .iter()
        .filter(|(k, _)| *k == 0)
        .map(|(_, n)| *n)
        .collect();
    assert_eq!(slow, [0, 1, 2]);
    s.shutdown(Duration::ZERO).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_key_is_halted_resumed_and_given_a_floor() {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let s: Sched = Scheduler::builder()
        .pool(pool(2, seen.clone(), Duration::from_millis(10)))
        .build()
        .await
        .unwrap();
    let bad = s
        .submit(
            Task::new((7, u64::MAX))
                .ordering_key("k7")
                .halt_on_failure()
                .retry(RetryPolicy::none()),
        )
        .await
        .unwrap();
    let next = s.submit(task(7, 1)).await.unwrap();
    assert!(bad.await.is_err());
    tokio::time::sleep(Duration::from_secs(1)).await;
    let info = s.ordering("default", "k7").await.unwrap().unwrap();
    assert!(info.halted);
    assert_eq!((info.tasks, info.head), (1, Some(next.id())));
    assert_eq!(seen.lock().unwrap().started, [(7, u64::MAX)]);
    assert_eq!(
        s.halted_orderings("default", 10).await.unwrap(),
        vec![Arc::<str>::from("k7")]
    );
    assert_eq!(s.snapshot().await.unwrap().pools[0].ordering.halted, 1);

    // A floor shows in the class the head runs at, without changing the task.
    s.set_ordering_floor("default", "k7", Some(Priority::Critical), false)
        .await
        .unwrap();
    let info = s.ordering("default", "k7").await.unwrap().unwrap();
    assert_eq!(
        (info.floor, info.effective),
        (Some(Priority::Critical), Some(Priority::Critical))
    );
    let head = s.task(next.id()).await.unwrap().unwrap();
    assert_eq!(head.priority, Priority::Normal);

    assert!(s.resume_ordering("default", "k7").await.unwrap());
    assert!(!s.resume_ordering("default", "k7").await.unwrap());
    next.await.unwrap();
    assert_eq!(s.ordering("default", "k7").await.unwrap(), None);

    // Unknown pools are an error; what cannot be combined is refused at submission.
    assert!(s.ordering("nope", "k").await.is_err());
    assert!(s.resume_ordering("nope", "k").await.is_err());
    let fixed = Task::new((1, 1))
        .ordering_key("k")
        .start_at(std::time::SystemTime::now() + Duration::from_secs(60));
    assert!(matches!(
        s.submit(fixed).await,
        Err(Error::Submit(SubmitError::Invalid(_)))
    ));
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_pool_a_class_and_a_key_are_paused_and_resumed() {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let s: Sched = Scheduler::builder()
        .pool(pool(4, seen.clone(), Duration::from_millis(10)))
        .build()
        .await
        .unwrap();
    assert!(
        s.pause("default", PauseTarget::Pool, "deploy")
            .await
            .unwrap()
    );
    assert!(
        !s.pause("default", PauseTarget::Pool, "deploy")
            .await
            .unwrap()
    );
    let held = s.submit(task(1, 0)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(s.status(held.id()).await.unwrap(), Some(TaskStatus::Ready));
    let snap = s.snapshot().await.unwrap();
    assert_eq!(snap.pools[0].pauses.pool.as_deref(), Some("deploy"));
    assert_eq!(snap.pools[0].paused_tasks, 1);
    assert!(s.resume("default", PauseTarget::Pool).await.unwrap());
    assert!(!s.resume("default", PauseTarget::Pool).await.unwrap());
    held.await.unwrap();

    // A class, and an ordering key.
    let low = PauseTarget::Class(Priority::Low);
    assert!(
        s.pause("default", low.clone(), "backfill off")
            .await
            .unwrap()
    );
    let key = PauseTarget::Ordering("k2".into());
    assert!(
        s.pause("default", key.clone(), "customer asked")
            .await
            .unwrap()
    );
    let a = s.submit(task(3, 0).priority(Priority::Low)).await.unwrap();
    let b = s.submit(task(2, 0)).await.unwrap();
    let c = s.submit(task(4, 0)).await.unwrap();
    c.await.unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        s.paused_orderings("default", 10).await.unwrap(),
        vec![(Arc::<str>::from("k2"), Arc::<str>::from("customer asked"))]
    );
    let info = s.ordering("default", "k2").await.unwrap().unwrap();
    assert_eq!(info.paused.as_deref(), Some("customer asked"));
    assert_eq!(seen.lock().unwrap().started, [(1, 0), (4, 0)]);
    assert!(s.resume("default", low).await.unwrap());
    // An ordering key is resumed by either call.
    assert!(s.resume_ordering("default", "k2").await.unwrap());
    assert!(!s.resume("default", key).await.unwrap());
    a.await.unwrap();
    b.await.unwrap();
    assert!(s.pause("nope", PauseTarget::Pool, "x").await.is_err());
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_executor_releases_a_task_without_using_a_retry() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = Arc::new(AtomicU32::new(0));
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let (c, a) = (calls.clone(), attempts.clone());
    let s: Scheduler<u32, u32> = Scheduler::builder()
        .pool(
            Pool::fixed("default", 2)
                .configure(|p| p.max_releases = 3)
                .executor_fn(move |ctx, x: Arc<u32>| {
                    let (c, a) = (c.clone(), a.clone());
                    async move {
                        a.lock().unwrap().push(ctx.attempt);
                        // The first three attempts are given back, the fourth fails once,
                        // the fifth succeeds.
                        match c.fetch_add(1, Ordering::SeqCst) {
                            0..=2 => Err(TaskError::release(Duration::from_secs(2))),
                            3 => Err(TaskError::retryable("flaky")),
                            _ => Ok(*x * 2),
                        }
                    }
                }),
        )
        .build()
        .await
        .unwrap();
    // Two attempts are allowed: the three released ones do not count.
    let policy = RetryPolicy {
        max_attempts: 2,
        initial_backoff: Duration::from_millis(100),
        ..Default::default()
    };
    let start = Instant::now();
    let h = s.submit(Task::new(21).retry(policy)).await.unwrap();
    assert_eq!(*h.await.unwrap(), 42);
    assert_eq!(*attempts.lock().unwrap(), [1, 2, 3, 4, 5]);
    assert!(
        start.elapsed() >= Duration::from_secs(6),
        "three delays of two seconds"
    );
    let snap = s.snapshot().await.unwrap();
    assert_eq!((snap.totals.releases, snap.totals.retries), (3, 1));
    // With the task's own limit of one, the second release is a failure.
    calls.store(0, Ordering::SeqCst);
    let h = s
        .submit(Task::new(1).retry(RetryPolicy::none()).max_releases(1))
        .await
        .unwrap();
    let e = h.await.unwrap_err();
    assert_eq!(e.status(), Some(TaskStatus::DeadLettered), "{e}");
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}
