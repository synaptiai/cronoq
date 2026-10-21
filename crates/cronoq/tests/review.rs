//! Regression tests for runtime review findings (each failed before its fix).

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cronoq::*;
use tokio::sync::Notify;

type Sched = Scheduler<u64, u64>;

fn pending_pool(n: u32) -> Pool<u64, u64> {
    Pool::fixed("default", n).executor_fn(|ctx: TaskContext, x: Arc<u64>| async move {
        ctx.cancelled().await;
        Ok::<_, TaskError>(*x)
    })
}

fn echo_pool(n: u32) -> Pool<u64, u64> {
    Pool::fixed("default", n).executor_fn(|_c, x: Arc<u64>| async move { Ok::<_, TaskError>(*x) })
}

// --- Heartbeat floods must not starve commands (biased select + one message per heartbeat).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_flood_does_not_starve_commands() {
    let stop = Arc::new(AtomicBool::new(false));
    let st = stop.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 3).executor_fn(move |ctx: TaskContext, x: Arc<u64>| {
                let st = st.clone();
                async move {
                    while !st.load(Ordering::Relaxed) {
                        for _ in 0..10_000 {
                            ctx.heartbeat();
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, TaskError>(*x)
                }
            }),
        )
        .build()
        .await
        .unwrap();
    for i in 0..3 {
        s.enqueue(Task::new(i)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut worst = Duration::ZERO;
    for _ in 0..5 {
        let t = std::time::Instant::now();
        let r = tokio::time::timeout(Duration::from_secs(3), s.status(TaskId(1))).await;
        worst = worst.max(t.elapsed());
        assert!(r.is_ok(), "status() starved by heartbeat feedback");
    }
    stop.store(true, Ordering::Relaxed);
    assert!(
        worst < Duration::from_millis(500),
        "status() latency {worst:?}"
    );
}

// --- TaskHandle::cancel takes effect before later commands.
#[tokio::test]
async fn handle_cancel_is_ordered_before_later_commands() {
    let s: Sched = Scheduler::builder()
        .pool(pending_pool(1))
        .build()
        .await
        .unwrap();
    let h = s.submit(Task::new(1)).await.unwrap();
    tokio::task::yield_now().await;
    h.cancel();
    let st = s.status(h.id()).await.unwrap();
    assert_eq!(st, Some(TaskStatus::Cancelled));
}

// --- Far-future and huge times saturate instead of wrapping.
#[tokio::test]
async fn far_future_start_does_not_run_now() {
    let ran = Arc::new(AtomicU32::new(0));
    let r = ran.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(move |_ctx, x: Arc<u64>| {
                r.fetch_add(1, Ordering::SeqCst);
                async move { Ok::<_, TaskError>(*x) }
            }),
        )
        .build()
        .await
        .unwrap();
    let t = UNIX_EPOCH + Duration::from_secs(9_300_000_000); // year ~2265
    s.enqueue(Task::new(1).start_at(t)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn huge_deadline_is_reported_in_the_future() {
    let seen = Arc::new(Mutex::new(None));
    let sn = seen.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(move |ctx: TaskContext, x: Arc<u64>| {
                *sn.lock().unwrap() = ctx.deadline;
                async move { Ok::<_, TaskError>(*x) }
            }),
        )
        .build()
        .await
        .unwrap();
    let h = s
        .submit(Task::new(1).deadline_in(Duration::from_secs(400 * 365 * 86400)))
        .await
        .unwrap();
    h.await.unwrap();
    let d = seen.lock().unwrap().unwrap();
    assert!(d > SystemTime::now(), "deadline reported as {d:?}");
}

// --- Recurring templates with one-shot fields are rejected instead of silently dropped.
#[tokio::test]
async fn recurring_template_with_deadline_is_rejected() {
    let s: Sched = Scheduler::builder()
        .pool(echo_pool(1))
        .build()
        .await
        .unwrap();
    let r = s
        .add_recurring(Recurring::new(
            "r",
            "* * * * * *",
            Task::new(1).deadline_in(Duration::from_secs(10)),
        ))
        .await;
    assert!(matches!(r, Err(Error::Config(m)) if m.contains("deadline")));
    assert!(
        s.add_recurring(Recurring::new("ok", "* * * * * *", Task::new(1)))
            .await
            .is_ok()
    );
}

// --- A synchronous panic in an executor fails only that task.
#[tokio::test]
async fn sync_executor_panic_fails_only_that_task() {
    let s: Sched = Scheduler::builder()
        .pool(Pool::fixed("default", 1).executor_fn(|_ctx, x: Arc<u64>| {
            assert!(*x != 13, "bad payload");
            async move { Ok::<_, TaskError>(*x) }
        }))
        .build()
        .await
        .unwrap();
    let e = s.submit(Task::new(13)).await.unwrap().await.unwrap_err();
    assert_eq!(e.status(), Some(TaskStatus::Failed));
    assert!(e.to_string().contains("bad payload"), "{e}");
    assert_eq!(*s.submit(Task::new(1)).await.unwrap().await.unwrap(), 1);
}

// --- Stores used by the persistence tests below.
/// Lets a test keep a reference to the store it hands to the scheduler.
struct Shared(Arc<ScriptedStore>);

impl std::ops::Deref for Shared {
    type Target = ScriptedStore;
    fn deref(&self) -> &ScriptedStore {
        &self.0
    }
}

#[derive(Default)]
struct ScriptedStore {
    /// Number of commits that fail with `Unavailable` before succeeding.
    unavailable: AtomicU32,
    /// Number of commits that fail with `Other`.
    other: AtomicU32,
    /// Panic on the next commit.
    panic_once: AtomicBool,
    /// While set, commits wait for `gate`.
    hold: AtomicBool,
    gate: Notify,
    commits: AtomicU32,
    released: AtomicBool,
    fail_recover: bool,
    outputs: Mutex<Vec<(TaskId, u64)>>,
}

#[async_trait]
impl Store<u64, u64> for Shared {
    async fn acquire_leadership(&self) -> Result<u16, StoreError> {
        Ok(1)
    }
    async fn release_leadership(&self) {
        self.released.store(true, Ordering::SeqCst);
    }
    async fn commit(
        &self,
        _: u16,
        _: TimeBase,
        _: &[Mutation<u64>],
        outs: &[(TaskId, &u64)],
    ) -> Result<(), StoreError> {
        if self.panic_once.swap(false, Ordering::SeqCst) {
            panic!("store bug");
        }
        while self.hold.load(Ordering::SeqCst) {
            self.gate.notified().await;
        }
        let dec = |c: &AtomicU32| {
            #[allow(deprecated)] // `try_update` (the new name) needs Rust 1.98; MSRV is 1.85
            c.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        };
        if dec(&self.unavailable) {
            return Err(StoreError::Unavailable("connection reset".into()));
        }
        if dec(&self.other) {
            return Err(StoreError::Other("constraint violated".into()));
        }
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.outputs
            .lock()
            .unwrap()
            .extend(outs.iter().map(|(i, o)| (*i, **o)));
        Ok(())
    }
    async fn recover(&self, _: TimeBase) -> Result<Recovered<u64>, StoreError> {
        if self.fail_recover {
            return Err(StoreError::Other("corrupt".into()));
        }
        Ok(Recovered::default())
    }
    async fn load_output(&self, id: TaskId) -> Result<Option<u64>, StoreError> {
        Ok(self
            .outputs
            .lock()
            .unwrap()
            .iter()
            .find(|(i, _)| *i == id)
            .map(|(_, o)| *o))
    }
}

async fn with_store(store: Arc<ScriptedStore>) -> Sched {
    Scheduler::builder()
        .pool(
            Pool::fixed("default", 1)
                .executor_fn(|_c, x: Arc<u64>| async move { Ok::<_, TaskError>(*x * 10) }),
        )
        .store(Shared(store))
        .build()
        .await
        .unwrap()
}

// --- A transient outage is retried without stopping the scheduler, and the engine keeps
//     answering queries meanwhile.
#[tokio::test(start_paused = true)]
async fn transient_store_outage_is_survived() {
    let store = Arc::new(ScriptedStore::default());
    store.unavailable.store(20, Ordering::SeqCst); // several seconds of backoff
    let s = with_store(store.clone()).await;
    let s2 = s.clone();
    let submit = tokio::spawn(async move { s2.submit(Task::new(1)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Not yet durable, but the engine is responsive.
    assert!(!submit.is_finished());
    // (The engine already assigned it; dispatch itself waits for the commit.)
    assert!(s.status(TaskId((1 << 48) | 1)).await.unwrap().is_some());
    let h = submit.await.unwrap().unwrap();
    assert_eq!(*h.await.unwrap(), 10);
    assert!(s.submit(Task::new(2)).await.is_ok());
}

// --- A persistent non-transient failure stops the scheduler and reports the store error
//     to the waiting submitter (not `Shutdown`).
#[tokio::test(start_paused = true)]
async fn fatal_store_error_is_reported_to_submitters() {
    let store = Arc::new(ScriptedStore::default());
    store.other.store(100, Ordering::SeqCst);
    let s = with_store(store).await;
    let r = s.submit(Task::new(1)).await;
    assert!(
        matches!(r, Err(Error::Store(StoreError::Other(_)))),
        "{r:?}"
    );
    assert!(matches!(
        s.shutdown(Duration::ZERO).await,
        Err(Error::Store(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn outage_longer_than_limit_is_fatal() {
    let store = Arc::new(ScriptedStore::default());
    store.unavailable.store(u32::MAX, Ordering::SeqCst);
    let s: Sched = Scheduler::builder()
        .pool(echo_pool(1))
        .store(Shared(store))
        .max_store_outage(Duration::from_secs(10))
        .build()
        .await
        .unwrap();
    let r = s.submit(Task::new(1)).await;
    assert!(
        matches!(r, Err(Error::Store(StoreError::Unavailable(_)))),
        "{r:?}"
    );
}

// --- A panic inside a store is contained and retried.
#[tokio::test]
async fn store_panic_does_not_kill_the_scheduler() {
    let store = Arc::new(ScriptedStore::default());
    store.panic_once.store(true, Ordering::SeqCst);
    let s = with_store(store).await;
    assert!(s.submit(Task::new(1)).await.is_ok());
    assert!(s.submit(Task::new(2)).await.is_ok());
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

// --- Cancel and recurring changes are acknowledged only once durable.
#[tokio::test]
async fn cancel_and_recurring_wait_for_durability() {
    let store = Arc::new(ScriptedStore::default());
    let s = with_store(store.clone()).await;
    s.enqueue(Task::new(1).key("k").start_in(Duration::from_secs(3600)))
        .await
        .unwrap();
    store.hold.store(true, Ordering::SeqCst);
    let s2 = s.clone();
    let cancel = tokio::spawn(async move { s2.cancel("k").await });
    let s3 = s.clone();
    let add = tokio::spawn(async move {
        s3.add_recurring(Recurring::new("r", "0 0 1 1 *", Task::new(1)))
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !cancel.is_finished() && !add.is_finished(),
        "acked before commit"
    );
    store.hold.store(false, Ordering::SeqCst);
    store.gate.notify_waiters();
    assert!(cancel.await.unwrap().unwrap());
    add.await.unwrap().unwrap();
}

// --- Leadership is released when recovery fails, so a standby can take over.
#[tokio::test]
async fn failed_recovery_releases_leadership() {
    let store = Arc::new(ScriptedStore {
        fail_recover: true,
        ..Default::default()
    });
    let r = Scheduler::<u64, u64>::builder()
        .pool(echo_pool(1))
        .store(Shared(store.clone()))
        .build()
        .await;
    assert!(matches!(r, Err(Error::Store(_))));
    assert!(store.released.load(Ordering::SeqCst));
}

// --- Duplicates of succeeded tasks resolve with the output (cache, then store).
#[tokio::test]
async fn duplicate_of_succeeded_task_gets_output() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1)
                .executor_fn(|_c, x: Arc<u64>| async move { Ok::<_, TaskError>(*x * 10) }),
        )
        .build()
        .await
        .unwrap();
    let h = s.submit(Task::new(1).key("k")).await.unwrap();
    assert_eq!(*h.await.unwrap(), 10);
    let h2 = s.submit(Task::new(1).key("k")).await.unwrap();
    assert!(h2.is_duplicate());
    assert_eq!(*h2.await.unwrap(), 10);
    let h3 = s.handle("k").await.unwrap().unwrap();
    assert!(!h3.is_duplicate());
    assert_eq!(*h3.await.unwrap(), 10);
}

#[tokio::test]
async fn output_comes_from_store_when_not_cached() {
    let store = Arc::new(ScriptedStore::default());
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1)
                .executor_fn(|_c, x: Arc<u64>| async move { Ok::<_, TaskError>(*x * 10) }),
        )
        .store(Shared(store))
        .output_cache(0)
        .build()
        .await
        .unwrap();
    assert_eq!(
        *s.submit(Task::new(4).key("k"))
            .await
            .unwrap()
            .await
            .unwrap(),
        40
    );
    assert_eq!(
        *s.submit(Task::new(4).key("k"))
            .await
            .unwrap()
            .await
            .unwrap(),
        40
    );
    let mem: Sched = Scheduler::builder()
        .pool(echo_pool(1))
        .output_cache(0)
        .build()
        .await
        .unwrap();
    mem.submit(Task::new(1).key("k"))
        .await
        .unwrap()
        .await
        .unwrap();
    let e = mem.submit(Task::new(1).key("k")).await.unwrap().await;
    assert!(
        matches!(e, Err(TaskFailure::OutputUnavailable { .. })),
        "{e:?}"
    );
}

// --- BlockingScheduler works from inside a runtime.
#[tokio::test]
async fn blocking_scheduler_inside_a_runtime() {
    let s = Scheduler::<u64, u64>::builder()
        .pool(echo_pool(1))
        .build_blocking()
        .unwrap();
    let h = s.submit(Task::new(5)).unwrap();
    assert_eq!(*h.wait().unwrap(), 5);
    s.set_capacity("default", 2).unwrap();
    assert_eq!(
        s.enqueue_many(vec![Task::new(1), Task::new(2)])
            .unwrap()
            .len(),
        2
    );
    s.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn blocking_handle_after_shutdown_and_drop() {
    let s = Scheduler::<u64, u64>::builder()
        .pool(pending_pool(1))
        .build_blocking()
        .unwrap();
    let mut h = s.submit(Task::new(1)).unwrap();
    s.shutdown(Duration::from_millis(10)).unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        h.wait_timeout(Duration::from_millis(10))
    }));
    assert!(r.is_ok());
    let s = Scheduler::<u64, u64>::builder()
        .pool(pending_pool(1))
        .build_blocking()
        .unwrap();
    let mut h = s.submit(Task::new(1)).unwrap();
    drop(s);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        h.wait_timeout(Duration::from_millis(10))
    }));
    assert!(r.is_ok());
}

// --- Dropping the last Scheduler lets running attempts finish (drop grace).
#[tokio::test(start_paused = true)]
async fn dropping_scheduler_lets_running_work_finish() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(|_c, x: Arc<u64>| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok::<_, TaskError>(*x)
            }),
        )
        .build()
        .await
        .unwrap();
    let h = s.submit(Task::new(7)).await.unwrap();
    let queued = s.submit(Task::new(8)).await.unwrap();
    drop(s);
    assert_eq!(h.await.map(|v| *v), Ok(7));
    assert_eq!(queued.await.unwrap_err(), TaskFailure::Shutdown);
}

// --- Concurrent shutdowns all see the driver's result.
#[tokio::test(start_paused = true)]
async fn every_shutdown_caller_sees_the_result() {
    let store = Arc::new(ScriptedStore::default());
    store.other.store(100, Ordering::SeqCst);
    let s = with_store(store).await;
    let _ = s.enqueue(Task::new(1)).await;
    let (a, b) = tokio::join!(s.shutdown(Duration::ZERO), s.shutdown(Duration::ZERO));
    assert!(a.is_err() && b.is_err(), "{a:?} {b:?}");
}

// --- enqueue_many and submit_batch wait for space like submit.
#[tokio::test(start_paused = true)]
async fn many_and_batch_wait_for_space() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1)
                .executor_fn(|_c, x: Arc<u64>| async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<_, TaskError>(*x)
                })
                .configure(|c| c.bounds.max_pending = Some(2)),
        )
        .build()
        .await
        .unwrap();
    let r = s
        .enqueue_many((0..6).map(Task::new).collect())
        .await
        .unwrap();
    assert!(r.iter().all(|x| x.is_ok()), "{r:?}");
    let hs = s
        .submit_batch(vec![Task::new(10), Task::new(11)])
        .await
        .unwrap();
    for h in hs {
        h.await.unwrap();
    }
    // Larger than the bound: rejected instead of waiting forever.
    let e = s
        .submit_batch((0..3).map(Task::new).collect())
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Submit(SubmitError::Overloaded)), "{e:?}");
}

// --- Out-of-order scale responses: a slow old response does not override a newer one.
struct SlowFirst {
    calls: AtomicU32,
    log: Mutex<Vec<u32>>,
}

#[async_trait]
impl Scaler for SlowFirst {
    async fn scale_to(&self, _pool: &str, desired: u32) -> Result<u32, ScaleError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            tokio::time::sleep(Duration::from_secs(30)).await;
            return Ok(8);
        }
        self.log.lock().unwrap().push(desired);
        Ok(desired)
    }
}

#[tokio::test(start_paused = true)]
async fn stale_scale_responses_are_ignored() {
    let scaler = Arc::new(SlowFirst {
        calls: AtomicU32::new(0),
        log: Mutex::new(Vec::new()),
    });
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::new("default")
                .workers(1, 1, 16)
                .scaler_arc(scaler.clone())
                .autoscale(AutoscaleConfig {
                    drain_time: Duration::from_secs(10),
                    cooldown: Duration::from_secs(1),
                    stabilization_window: Duration::from_secs(1),
                    ..Default::default()
                })
                .executor_fn(|_c, x: Arc<u64>| async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok::<_, TaskError>(*x)
                }),
        )
        .build()
        .await
        .unwrap();
    for i in 0..40 {
        s.enqueue(Task::new(i)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(40)).await;
    let cap = s.snapshot().await.unwrap().pools[0].capacity;
    let last = *scaler.log.lock().unwrap().last().unwrap();
    assert_eq!(cap, last, "stale response overrode capacity");
}

// --- Builder order does not matter for start options.
#[test]
fn start_options_are_order_independent() {
    let a = format!(
        "{:?}",
        Task::new(1u64)
            .start_tolerance(Duration::from_secs(60))
            .start_in(Duration::from_secs(1))
    );
    let b = format!(
        "{:?}",
        Task::new(1u64)
            .start_in(Duration::from_secs(1))
            .start_tolerance(Duration::from_secs(60))
    );
    assert_eq!(a, b);
}

// --- Executors can be unit-tested with a detached context.
#[tokio::test]
async fn task_context_for_tests() {
    let exec = FnExecutor::new(|ctx: TaskContext, x: Arc<u64>| async move {
        ctx.heartbeat();
        Ok::<_, TaskError>(*x + ctx.attempt as u64)
    });
    let ctx = TaskContext::for_test(TaskId(7));
    assert_eq!(
        Executor::<u64, u64>::execute(&exec, ctx.clone(), Arc::new(1)).await,
        Ok(2)
    );
    ctx.cancellation_token().cancel();
    assert!(ctx.is_cancelled());
}

// --- Invalid inputs are rejected at submission.
#[tokio::test]
async fn unstorable_values_are_rejected() {
    let s: Sched = Scheduler::builder()
        .pool(echo_pool(1))
        .build()
        .await
        .unwrap();
    let e = s.submit(Task::new(1).key("bad\0key")).await.unwrap_err();
    assert!(matches!(e, Error::Submit(SubmitError::Invalid(_))));
    let r = RetryPolicy {
        multiplier: f64::NAN,
        ..Default::default()
    };
    assert!(s.submit(Task::new(1).retry(r)).await.is_err());
}

// --- Events capacity is configurable.
#[tokio::test]
async fn events_capacity_is_configurable() {
    let s: Sched = Scheduler::builder()
        .pool(echo_pool(1))
        .events_capacity(1)
        .build()
        .await
        .unwrap();
    let mut ev = s.events();
    for i in 0..3 {
        s.submit(Task::new(i)).await.unwrap().await.unwrap();
    }
    // Capacity 1: the receiver lagged and lost older events.
    assert!(matches!(
        ev.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));
}
