use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cronoq::*;
use tokio::sync::Notify;
use tokio::time::Instant;

type Sched = Scheduler<u64, u64>;

fn double() -> Pool<u64, u64> {
    Pool::fixed("default", 4)
        .executor_fn(|_ctx, x: Arc<u64>| async move { Ok::<_, TaskError>(*x * 2) })
}

#[tokio::test]
async fn submit_and_await_output() {
    let s: Sched = Scheduler::builder().pool(double()).build().await.unwrap();
    let handles: Vec<_> = futures(&s, 0..100).await;
    for (i, h) in handles.into_iter().enumerate() {
        assert_eq!(*h.await.unwrap(), i as u64 * 2);
    }
    let snap = s.snapshot().await.unwrap();
    assert_eq!(snap.totals.succeeded, 100);
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

async fn futures(s: &Sched, r: std::ops::Range<u64>) -> Vec<TaskHandle<u64>> {
    let mut v = Vec::new();
    for i in r {
        v.push(s.submit(Task::new(i)).await.unwrap());
    }
    v
}

#[tokio::test(start_paused = true)]
async fn priorities_order_dispatch() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(Notify::new());
    let (o, g) = (order.clone(), gate.clone());
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(move |_ctx, x: Arc<u64>| {
                let (o, g) = (o.clone(), g.clone());
                async move {
                    if *x == 0 {
                        g.notified().await;
                    }
                    o.lock().unwrap().push(*x);
                    Ok(*x)
                }
            }),
        )
        .build()
        .await
        .unwrap();
    let first = s.submit(Task::new(0)).await.unwrap();
    let mut hs = vec![first];
    for (x, p) in [
        (1, Priority::Low),
        (2, Priority::Normal),
        (3, Priority::Critical),
        (4, Priority::High),
    ] {
        hs.push(s.submit(Task::new(x).priority(p)).await.unwrap());
    }
    gate.notify_one();
    for h in hs {
        h.await.unwrap();
    }
    assert_eq!(*order.lock().unwrap(), vec![0, 3, 4, 2, 1]);
}

#[tokio::test(start_paused = true)]
async fn retries_then_dead_letter_and_permanent_failure() {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 2).executor_fn(move |ctx, x: Arc<u64>| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    match *x {
                        1 if ctx.attempt < 3 => Err(TaskError::retryable("flaky")),
                        2 => Err(TaskError::retryable("always")),
                        3 => Err(TaskError::permanent("bad input")),
                        _ => Ok(*x),
                    }
                }
            }),
        )
        .build()
        .await
        .unwrap();
    let ok = s.submit(Task::new(1)).await.unwrap();
    let dead = s.submit(Task::new(2)).await.unwrap();
    let bad = s.submit(Task::new(3)).await.unwrap();
    assert_eq!(*ok.await.unwrap(), 1);
    assert_eq!(
        dead.await.unwrap_err().status(),
        Some(TaskStatus::DeadLettered)
    );
    let e = bad.await.unwrap_err();
    assert_eq!(e.status(), Some(TaskStatus::Failed));
    assert!(e.to_string().contains("bad input"));
    assert_eq!(calls.load(Ordering::SeqCst), 3 + 3 + 1);
}

#[tokio::test(start_paused = true)]
async fn timeouts_abort_attempts() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(|ctx, x: Arc<u64>| async move {
                if ctx.attempt == 1 {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
                Ok(*x)
            }),
        )
        .build()
        .await
        .unwrap();
    let t0 = Instant::now();
    let h = s
        .submit(Task::new(7).timeout(Duration::from_secs(2)))
        .await
        .unwrap();
    assert_eq!(*h.await.unwrap(), 7);
    assert!(
        t0.elapsed() < Duration::from_secs(10),
        "second attempt ran after timeout + backoff"
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_reaches_running_executor() {
    let saw_cancel = Arc::new(AtomicU32::new(0));
    let sc = saw_cancel.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(move |ctx, _x: Arc<u64>| {
                let sc = sc.clone();
                async move {
                    let token = ctx.cancellation_token();
                    tokio::spawn(async move {
                        token.cancelled().await;
                        sc.fetch_add(1, Ordering::SeqCst);
                    });
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(0)
                }
            }),
        )
        .build()
        .await
        .unwrap();
    let running = s.submit(Task::new(1).key("long")).await.unwrap();
    let queued = s.submit(Task::new(2)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    queued.cancel();
    assert!(s.cancel("long").await.unwrap());
    assert_eq!(
        running.await.unwrap_err().status(),
        Some(TaskStatus::Cancelled)
    );
    assert_eq!(
        queued.await.unwrap_err().status(),
        Some(TaskStatus::Cancelled)
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(saw_cancel.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn panics_become_failures() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(|_ctx, x: Arc<u64>| async move {
                if *x == 1 {
                    panic!("kaboom");
                }
                Ok(*x)
            }),
        )
        .build()
        .await
        .unwrap();
    let e = s.submit(Task::new(1)).await.unwrap().await.unwrap_err();
    assert_eq!(e.status(), Some(TaskStatus::Failed));
    assert!(e.to_string().contains("kaboom"));
    assert_eq!(
        *s.submit(Task::new(2)).await.unwrap().await.unwrap(),
        2,
        "scheduler keeps working"
    );
}

#[tokio::test(start_paused = true)]
async fn dag_batch_runs_in_dependency_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let o = order.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 4).executor_fn(move |_ctx, x: Arc<u64>| {
                let o = o.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    o.lock().unwrap().push(*x);
                    Ok(*x)
                }
            }),
        )
        .build()
        .await
        .unwrap();
    let hs = s
        .submit_batch(vec![
            Task::new(3).key("c").after("a").after("b"),
            Task::new(1).key("a"),
            Task::new(2).key("b").after("a"),
        ])
        .await
        .unwrap();
    for h in hs {
        h.await.unwrap();
    }
    assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    let dup = s.submit(Task::new(9).key("a")).await.unwrap();
    assert!(dup.is_duplicate());
}

#[tokio::test(start_paused = true)]
async fn full_queue_waits_for_space() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1)
                .executor_fn(|_ctx, x: Arc<u64>| async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(*x)
                })
                .configure(|c| c.bounds.max_pending = Some(2)),
        )
        .build()
        .await
        .unwrap();
    let mut joins = Vec::new();
    for i in 0..20 {
        let s = s.clone();
        joins.push(tokio::spawn(async move {
            s.submit(Task::new(i)).await.unwrap().await.unwrap()
        }));
    }
    let mut total = 0;
    for j in joins {
        total += *j.await.unwrap();
    }
    assert_eq!(total, (0..20).sum());

    let r: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1)
                .executor_fn(|_ctx, x: Arc<u64>| async move {
                    tokio::time::sleep(Duration::from_secs(100)).await;
                    Ok(*x)
                })
                .configure(|c| {
                    c.bounds.max_pending = Some(1);
                    c.overflow = OverflowPolicy::Reject;
                }),
        )
        .build()
        .await
        .unwrap();
    r.enqueue(Task::new(1)).await.unwrap();
    r.enqueue(Task::new(2)).await.unwrap();
    let err = r.enqueue(Task::new(3)).await.unwrap_err();
    assert!(
        matches!(err, Error::Submit(SubmitError::Overloaded)),
        "{err:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn fixed_start_runs_on_time() {
    let started = Arc::new(Mutex::new(None));
    let st = started.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(move |_ctx, x: Arc<u64>| {
                let st = st.clone();
                async move {
                    st.lock().unwrap().get_or_insert(Instant::now());
                    Ok(*x)
                }
            }),
        )
        .build()
        .await
        .unwrap();
    let t0 = Instant::now();
    let h = s
        .submit(Task::new(1).start_in(Duration::from_secs(30)))
        .await
        .unwrap();
    h.await.unwrap();
    let at = started.lock().unwrap().unwrap() - t0;
    assert!(
        at >= Duration::from_secs(30) && at < Duration::from_secs(31),
        "{at:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn recurring_cron_fires() {
    let count = Arc::new(AtomicU32::new(0));
    let c = count.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 2).executor_fn(move |_ctx, _x: Arc<u64>| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(0)
                }
            }),
        )
        .build()
        .await
        .unwrap();
    s.add_recurring(Recurring::new("every-5s", "*/5 * * * * *", Task::new(0)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(31)).await;
    let n = count.load(Ordering::SeqCst);
    assert!((5..=7).contains(&n), "fired {n} times in 31s");
    assert!(s.remove_recurring("every-5s").await.unwrap());
    assert!(
        s.add_recurring(Recurring::new("bad", "nope", Task::new(0)))
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn autoscaling_adds_slots_under_load() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::new("default")
                .workers(1, 1, 16)
                .executor_fn(|_ctx, x: Arc<u64>| async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(*x)
                })
                .autoscale(AutoscaleConfig {
                    drain_time: Duration::from_secs(10),
                    ..Default::default()
                }),
        )
        .build()
        .await
        .unwrap();
    let t0 = Instant::now();
    let hs = futures(&s, 0..200).await;
    for h in hs {
        h.await.unwrap();
    }
    let took = t0.elapsed();
    let snap = s.snapshot().await.unwrap();
    assert!(
        snap.pools[0].capacity > 4,
        "capacity {}",
        snap.pools[0].capacity
    );
    assert!(
        took < Duration::from_secs(60),
        "200 x 1s tasks took {took:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn resources_rate_limit_across_pools() {
    let s: Sched = Scheduler::builder()
        .pool(Pool::fixed("a", 8).executor_fn(|_c, x: Arc<u64>| async move { Ok(*x) }))
        .pool(Pool::fixed("b", 8).executor_fn(|_c, x: Arc<u64>| async move { Ok(*x) }))
        .resource(ResourceConfig::token_bucket("api", 10.0, 1))
        .build()
        .await
        .unwrap();
    let t0 = Instant::now();
    let mut hs = Vec::new();
    for i in 0..20 {
        let pool = if i % 2 == 0 { "a" } else { "b" };
        hs.push(
            s.submit(Task::new(i).pool(pool).resource("api", 1))
                .await
                .unwrap(),
        );
    }
    for h in hs {
        h.await.unwrap();
    }
    let took = t0.elapsed().as_secs_f64();
    assert!((1.8..2.2).contains(&took), "20 tokens at 10/s took {took}");
}

#[tokio::test]
async fn events_stream_reports_transitions() {
    let s: Sched = Scheduler::builder()
        .pool(double())
        .emit_transitions(true)
        .build()
        .await
        .unwrap();
    let mut ev = s.events();
    let h = s.submit(Task::new(1)).await.unwrap();
    let id = h.id();
    h.await.unwrap();
    let mut seen = Vec::new();
    while let Ok(e) = ev.try_recv() {
        if e.id == id {
            seen.push(e.status);
        }
    }
    assert_eq!(
        seen,
        vec![
            TaskStatus::Blocked,
            TaskStatus::Ready,
            TaskStatus::Running,
            TaskStatus::Succeeded
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn heartbeats_keep_leases_alive() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1)
                .executor_fn(|ctx, x: Arc<u64>| async move {
                    for _ in 0..10 {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        if *x == 1 {
                            ctx.heartbeat();
                        }
                    }
                    Ok(*x)
                })
                .configure(|c| {
                    c.lease_ttl = Some(Duration::from_secs(5));
                    c.retry = RetryPolicy::none();
                }),
        )
        .build()
        .await
        .unwrap();
    assert_eq!(*s.submit(Task::new(1)).await.unwrap().await.unwrap(), 1);
    let e = s.submit(Task::new(2)).await.unwrap().await.unwrap_err();
    assert_eq!(e.status(), Some(TaskStatus::DeadLettered));
}

#[test]
fn blocking_api_works_without_a_runtime() {
    let s = Scheduler::<u64, u64>::builder()
        .pool(double())
        .build_blocking()
        .unwrap();
    let h = s.submit(Task::new(21)).unwrap();
    assert_eq!(*h.wait().unwrap(), 42);
    assert_eq!(
        s.status(TaskRef::Id(TaskId(1))).unwrap(),
        Some(TaskStatus::Succeeded)
    );
    s.shutdown(Duration::from_secs(1)).unwrap();
}

#[tokio::test]
async fn shutdown_resolves_pending_handles() {
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(|_c, x: Arc<u64>| async move {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(*x)
            }),
        )
        .build()
        .await
        .unwrap();
    let a = s.submit(Task::new(1)).await.unwrap();
    let b = s.submit(Task::new(2)).await.unwrap();
    s.shutdown(Duration::from_millis(50)).await.unwrap();
    assert_eq!(a.await.unwrap_err(), TaskFailure::Shutdown);
    assert_eq!(b.await.unwrap_err(), TaskFailure::Shutdown);
    assert!(matches!(s.submit(Task::new(3)).await, Err(Error::Shutdown)));
}

// ------------------------------------------------------------------------------------------
// Persistence through the Store trait
// ------------------------------------------------------------------------------------------

/// Store that keeps the mutation log in memory and can simulate fencing.
#[derive(Default, Clone)]
struct LogStore {
    log: Arc<Mutex<Vec<Mutation<u64>>>>,
    outputs: Arc<Mutex<Vec<(TaskId, u64)>>>,
    epoch: Arc<AtomicU32>,
    fenced: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl Store<u64, u64> for LogStore {
    async fn acquire_leadership(&self) -> Result<u16, StoreError> {
        Ok(self.epoch.fetch_add(1, Ordering::SeqCst) as u16 + 1)
    }
    async fn commit(
        &self,
        _e: u16,
        _b: TimeBase,
        batch: &[Mutation<u64>],
        outs: &[(TaskId, &u64)],
    ) -> Result<(), StoreError> {
        if self.fenced.load(Ordering::SeqCst) {
            return Err(StoreError::Fenced);
        }
        self.log.lock().unwrap().extend(batch.iter().cloned());
        self.outputs
            .lock()
            .unwrap()
            .extend(outs.iter().map(|(i, o)| (*i, **o)));
        Ok(())
    }
    async fn recover(&self, _b: TimeBase) -> Result<Recovered<u64>, StoreError> {
        use std::collections::BTreeMap;
        let mut tasks: BTreeMap<TaskId, RecoveredTask<u64>> = BTreeMap::new();
        let mut rec = Recovered::default();
        for m in self.log.lock().unwrap().iter() {
            match m {
                Mutation::Submitted {
                    id,
                    seq,
                    arrival,
                    spec,
                } => {
                    rec.next_id = rec.next_id.max(id.0 + 1);
                    tasks.insert(
                        *id,
                        RecoveredTask {
                            id: *id,
                            seq: *seq,
                            arrival: *arrival,
                            spec: spec.clone(),
                            attempt: 0,
                        },
                    );
                }
                Mutation::Status {
                    id,
                    status,
                    attempt,
                    at,
                    ..
                } => {
                    if status.is_terminal() {
                        if let Some(t) = tasks.remove(id) {
                            rec.terminal.push(RecoveredTerminal {
                                id: *id,
                                key: t.spec.key,
                                status: *status,
                                at: *at,
                            });
                        }
                    } else if let Some(t) = tasks.get_mut(id) {
                        t.attempt = *attempt;
                    }
                }
                _ => {}
            }
        }
        // Engine times restart at zero in the new process.
        for t in tasks.values_mut() {
            t.arrival = Time::ZERO;
        }
        rec.tasks = tasks.into_values().collect();
        Ok(rec)
    }
}

fn slow_pool() -> Pool<u64, u64> {
    Pool::fixed("default", 1).executor_fn(|_c, x: Arc<u64>| async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(*x + 100)
    })
}

#[tokio::test(start_paused = true)]
async fn store_recovers_unfinished_work_after_restart() {
    let store = LogStore::default();
    let s: Sched = Scheduler::builder()
        .pool(slow_pool())
        .store(store.clone())
        .build()
        .await
        .unwrap();
    let first = s.submit(Task::new(1).key("one")).await.unwrap();
    for i in 2..=5 {
        s.enqueue(Task::new(i)).await.unwrap();
    }
    assert_eq!(*first.await.unwrap(), 101);
    // "Crash" (abort without finishing the queue).
    s.shutdown(Duration::ZERO).await.unwrap();
    assert!(
        store
            .outputs
            .lock()
            .unwrap()
            .contains(&(TaskId((1u64 << 48) | 1), 101))
    );

    let s2: Sched = Scheduler::builder()
        .pool(slow_pool())
        .store(store.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(s2.status("one").await.unwrap(), Some(TaskStatus::Succeeded));
    // The remaining four complete under the new leader; new ids carry the new epoch.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let snap = s2.snapshot().await.unwrap();
    assert_eq!(snap.totals.succeeded, 4, "{:?}", snap.totals);
    let fresh = s2.enqueue(Task::new(9)).await.unwrap();
    assert_eq!(fresh.id.0 >> 48, 2);
}

#[tokio::test(start_paused = true)]
async fn fenced_store_stops_the_scheduler() {
    let store = LogStore::default();
    let s: Sched = Scheduler::builder()
        .pool(slow_pool())
        .store(store.clone())
        .build()
        .await
        .unwrap();
    s.submit(Task::new(1)).await.unwrap().await.unwrap();
    store.fenced.store(true, Ordering::SeqCst);
    let _ = s.enqueue(Task::new(2)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(matches!(
        s.enqueue(Task::new(3)).await,
        Err(Error::Shutdown)
    ));
    let r = s.shutdown(Duration::ZERO).await;
    assert!(matches!(r, Err(Error::Store(StoreError::Fenced))), "{r:?}");
}

#[tokio::test(start_paused = true)]
async fn shutdown_stops_dispatching_queued_work() {
    let started = Arc::new(AtomicU32::new(0));
    let st = started.clone();
    let s: Sched = Scheduler::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(move |_c, x: Arc<u64>| {
                let st = st.clone();
                async move {
                    st.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Ok(*x)
                }
            }),
        )
        .build()
        .await
        .unwrap();
    for i in 0..5 {
        s.enqueue(Task::new(i)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let t0 = Instant::now();
    s.shutdown(Duration::from_secs(10)).await.unwrap();
    assert_eq!(
        started.load(Ordering::SeqCst),
        1,
        "queued tasks must not start during shutdown"
    );
    assert!(
        t0.elapsed() < Duration::from_secs(1),
        "shutdown waited {:?}",
        t0.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn executor_is_told_about_aborts_synchronously() {
    struct Probe(Arc<Mutex<Vec<LeaseId>>>);
    impl Executor<u64, u64> for Probe {
        fn execute(&self, _ctx: TaskContext, _p: Arc<u64>) -> BoxFuture<Result<u64, TaskError>> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(0)
            })
        }
        fn abort(&self, lease: LeaseId) {
            self.0.lock().unwrap().push(lease);
        }
    }
    let aborted = Arc::new(Mutex::new(Vec::new()));
    let s: Sched = Scheduler::builder()
        .pool(Pool::fixed("default", 1).executor(Probe(aborted.clone())))
        .build()
        .await
        .unwrap();
    let h = s
        .submit(Task::new(1).timeout(Duration::from_secs(1)).no_retry())
        .await
        .unwrap();
    assert_eq!(
        h.await.unwrap_err().status(),
        Some(TaskStatus::DeadLettered)
    );
    assert_eq!(*aborted.lock().unwrap(), vec![LeaseId(1)]);
}
