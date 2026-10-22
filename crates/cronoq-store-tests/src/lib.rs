//! Conformance tests every durable cronoq store must pass.
//!
//! A store crate implements [`Harness`] for its test setup and expands [`suite!`] in an
//! integration test; each function below becomes one test there.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cronoq::{Pool, Scheduler, Store, StoreError, StoreQueries, Task, TaskError, TimeBase};
use cronoq_core::*;

/// Opens store instances that share one fresh database.
#[async_trait]
pub trait Harness: Send + Sync + 'static {
    /// The store under test.
    type Store: Store<Bytes, Bytes> + StoreQueries<Bytes> + Clone;
    /// A new instance on the shared database, with short leadership timings.
    async fn open(&self) -> Self::Store;
}

/// Expands to one `#[tokio::test]` per conformance test. `$setup` is an async expression
/// returning `Option<impl Harness>`; `None` skips the test (e.g. no database available).
#[macro_export]
macro_rules! suite {
    ($setup:expr) => {
        $crate::suite!(@tests $setup;
            round_trips_every_mutation_and_field,
            times_are_rebased_and_old_terminals_pruned,
            second_instance_waits_and_fences_the_first,
            release_lets_a_standby_lead,
            key_reuse_in_same_batch,
            deps_on_parents_outside_retention_survive_failover,
            long_missed_starts_are_not_run_after_restart,
            message_does_not_depend_on_batching,
            codec_edges,
            outputs_are_loaded_from_the_store,
            ids_are_not_reused_after_prune,
            scheduler_resumes_after_restart_without_rerunning_finished_work,
        );
    };
    (@tests $setup:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                if let Some(h) = $setup.await {
                    $crate::$name(&h).await;
                }
            }
        )*
    };
}

const S: u64 = 1_000_000_000;
/// The runtime's time base: engine time is Unix time.
const BASE: TimeBase = TimeBase {
    unix_nanos_at_zero: 0,
};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn hours_ago(h: u64) -> u64 {
    now() - h * 3600 * S
}

fn base_at(unix: u64) -> TimeBase {
    TimeBase {
        unix_nanos_at_zero: unix as i64,
    }
}

fn submitted_at(id: u64, seq: u64, at: u64, spec: TaskSpec<Bytes>) -> Mutation<Bytes> {
    Mutation::Submitted {
        id: TaskId(id),
        seq,
        arrival: Time(at),
        spec,
    }
}

fn submitted(id: u64, seq: u64, spec: TaskSpec<Bytes>) -> Mutation<Bytes> {
    submitted_at(id, seq, now(), spec)
}

fn status(id: u64, st: TaskStatus, attempt: u32, at: u64, msg: Option<&str>) -> Mutation<Bytes> {
    Mutation::Status {
        id: TaskId(id),
        status: st,
        attempt,
        at: Time(at),
        message: msg.map(Into::into),
    }
}

type Runs = Arc<Mutex<HashMap<String, u32>>>;

fn pool(runs: Runs, delay: Duration) -> Pool<Bytes, Bytes> {
    Pool::fixed("default", 2).executor_fn(move |_ctx, p: Arc<Bytes>| {
        let runs = runs.clone();
        async move {
            tokio::time::sleep(delay).await;
            *runs
                .lock()
                .unwrap()
                .entry(String::from_utf8_lossy(&p).into_owned())
                .or_default() += 1;
            Ok::<_, TaskError>(Bytes::from(format!("out:{}", String::from_utf8_lossy(&p))))
        }
    })
}

async fn scheduler<S: Store<Bytes, Bytes>>(
    store: S,
    runs: Runs,
    delay: Duration,
) -> Scheduler<Bytes, Bytes> {
    Scheduler::<Bytes, Bytes>::builder()
        .pool(pool(runs, delay))
        .store(store)
        .build()
        .await
        .unwrap()
}

async fn until_status(s: &Scheduler<Bytes, Bytes>, key: &str, want: TaskStatus) {
    for _ in 0..200 {
        if s.status(key).await.unwrap() == Some(want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{key} did not reach {want}");
}

fn full_spec() -> TaskSpec<Bytes> {
    TaskSpec::new(Bytes::from_static(b"\x00payload\xff"))
        .kind("llm")
        .pool("gpu")
        .priority(Priority::High)
        .key("job-1")
        .deadline(Time(50 * S))
        .fixed_start(FixedStart {
            at: Time(30 * S),
            tolerance: Duration::from_millis(1500),
            on_miss: MissPolicy::Skip,
        })
        .duration_hint(Duration::from_millis(2500))
        .timeout(Duration::from_secs(9))
        .retry(RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(200),
            multiplier: 1.5,
            max_backoff: Duration::from_secs(7),
            jitter: false,
        })
        .dep(Dep::on_success(TaskId(7)))
        .dep(Dep::on_failure("parent"))
        .dep(Dep::on_complete("other"))
        .resource("api", 2)
        .resource("gpu_mem", 8)
        .tag("tenant", "acme")
        .tag("traceparent", "00-abc-def-01")
        .label("gpu", "a100")
        .label("zone", "eu")
        .tenant("acme")
}

/// Every mutation kind and spec field survives a commit and recovery; lookups by id and key
/// see the latest state; re-emitted submissions are idempotent.
pub async fn round_trips_every_mutation_and_field<H: Harness>(h: &H) {
    let store = h.open().await;
    let epoch = store.acquire_leadership().await.unwrap();
    assert_eq!(epoch, 1);
    let base = base_at(now());
    let recurring = RecurringSpec {
        name: "nightly".into(),
        cron: "0 3 * * *".into(),
        tz: Some("Europe/Lisbon".into()),
        template: TaskSpec::new(Bytes::from_static(b"tpl"))
            .kind("report")
            .pool("gpu")
            .retry(RetryPolicy::none())
            .key("nightly")
            .tag("a", "b")
            .label("gpu", "a100")
            .tenant("ops"),
        deadline_after: Some(Duration::from_millis(1500)),
        overlap: OverlapPolicy::QueueOnSuccess,
        tolerance: Duration::from_secs(5),
        on_miss: MissPolicy::Fail,
    };
    let gone = RecurringSpec {
        name: "gone".into(),
        ..recurring.clone()
    };
    let st = |id, s: TaskStatus, at| status(id, s, 1, at, Some(&s.to_string()));
    let batch = vec![
        submitted_at(10, 0, S, full_spec()),
        submitted_at(11, 1, S, TaskSpec::new(Bytes::from_static(b"b"))),
        submitted_at(
            12,
            2,
            S,
            TaskSpec::new(Bytes::from_static(b"c")).key("done"),
        ),
        st(11, TaskStatus::Running, 2 * S),
        st(11, TaskStatus::Retrying, 3 * S),
        st(12, TaskStatus::Running, 2 * S),
        st(12, TaskStatus::Succeeded, 4 * S),
        Mutation::RecurringUpsert {
            spec: recurring.clone(),
        },
        Mutation::RecurringUpsert { spec: gone },
        Mutation::RecurringNext {
            name: "nightly".into(),
            next: Some(Time(3600 * S)),
            last: Some(TaskId(12)),
        },
        Mutation::RecurringRemove {
            name: "gone".into(),
        },
    ];
    let out = Bytes::from_static(b"result");
    store
        .commit(epoch, base, &batch, &[(TaskId(12), &out)])
        .await
        .unwrap();

    let rec = store.recover(base).await.unwrap();
    assert_eq!(rec.next_id, 13);
    assert_eq!(rec.tasks.len(), 2);
    let t = &rec.tasks[0];
    assert_eq!(
        (t.id, t.seq, t.arrival, t.attempt),
        (TaskId(10), 0, Time(S), 0)
    );
    let (a, b) = (&t.spec, full_spec());
    assert_eq!(*a.payload, *b.payload);
    assert_eq!(
        (&*a.kind, &*a.pool, a.priority, a.key.as_deref()),
        ("llm", "gpu", Priority::High, Some("job-1"))
    );
    assert_eq!(a.deadline, b.deadline);
    assert_eq!(a.fixed_start, b.fixed_start);
    assert_eq!(
        (a.duration_hint, a.timeout, a.retry),
        (b.duration_hint, b.timeout, b.retry)
    );
    assert_eq!(a.deps, b.deps);
    assert_eq!(a.resources, b.resources);
    assert_eq!(a.tags, b.tags);
    assert_eq!(a.labels, b.labels);
    assert_eq!(a.tenant, b.tenant);
    let retrying = &rec.tasks[1];
    assert_eq!((retrying.id, retrying.attempt), (TaskId(11), 1));
    assert_eq!(rec.terminal.len(), 1);
    let done = &rec.terminal[0];
    assert_eq!(
        (done.id, done.key.as_deref(), done.status, done.at),
        (TaskId(12), Some("done"), TaskStatus::Succeeded, Time(4 * S))
    );

    assert_eq!(rec.recurring.len(), 1);
    let stored = &rec.recurring[0];
    let r = &stored.spec;
    assert_eq!(stored.next, Some(Time(3600 * S)));
    assert_eq!(stored.last, Some(TaskId(12)));
    assert_eq!(r.deadline_after, Some(Duration::from_millis(1500)));
    assert_eq!(r.template.key.as_deref(), Some("nightly"));
    assert_eq!(
        (&*r.name, &*r.cron, r.tz.as_deref()),
        ("nightly", "0 3 * * *", Some("Europe/Lisbon"))
    );
    assert_eq!(
        (r.overlap, r.tolerance, r.on_miss),
        (
            OverlapPolicy::QueueOnSuccess,
            Duration::from_secs(5),
            MissPolicy::Fail
        )
    );
    assert_eq!(*r.template.payload, Bytes::from_static(b"tpl"));
    assert_eq!(
        (&*r.template.kind, &*r.template.pool, r.template.retry),
        ("report", "gpu", Some(RetryPolicy::none()))
    );
    assert_eq!(r.template.tags, recurring.template.tags);
    assert_eq!(r.template.labels, recurring.template.labels);
    assert_eq!(r.template.tenant, recurring.template.tenant);

    let rec12 = store
        .get_task(&TaskRef::Key("done".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (rec12.id, rec12.status, rec12.attempt),
        (TaskId(12), Some(TaskStatus::Succeeded), 1)
    );
    assert_eq!(rec12.output, Some(out));
    assert_eq!(rec12.message.as_deref(), Some("succeeded"));
    let rec10 = store
        .get_task(&TaskRef::Id(TaskId(10)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((rec10.status, rec10.pool.as_str()), (None, "gpu"));
    assert!(
        store
            .get_task(&TaskRef::Id(TaskId(99)))
            .await
            .unwrap()
            .is_none()
    );

    store
        .commit(epoch, base, &[submitted_at(10, 0, S, full_spec())], &[])
        .await
        .unwrap();
    assert_eq!(store.recover(base).await.unwrap().tasks.len(), 2);
}

/// Times are stored as wall-clock time, so a later time base maps them back correctly;
/// terminal records outside retention are not loaded and can be pruned.
pub async fn times_are_rebased_and_old_terminals_pruned<H: Harness>(h: &H) {
    let store = h.open().await;
    let epoch = store.acquire_leadership().await.unwrap();
    let now = now();
    let old_base = base_at(now - 2 * 3600 * S); // engine started two hours ago
    let batch = vec![
        submitted_at(
            1,
            0,
            S,
            TaskSpec::new(Bytes::new()).deadline(Time(2 * 3600 * S + 60 * S)),
        ),
        submitted_at(2, 1, S, TaskSpec::new(Bytes::new()).key("ancient")),
        status(2, TaskStatus::Failed, 1, 10 * S, None), // finished ~2h ago
        submitted_at(3, 2, S, TaskSpec::new(Bytes::new())),
        status(3, TaskStatus::Running, 1, 2 * 3600 * S - S, None),
    ];
    store.commit(epoch, old_base, &batch, &[]).await.unwrap();

    let rec = store.recover(base_at(now)).await.unwrap();
    let t1 = rec.tasks.iter().find(|t| t.id == TaskId(1)).unwrap();
    assert_eq!(t1.arrival, Time::ZERO);
    assert_eq!(t1.spec.deadline, Some(Time(60 * S)));
    // A task persisted as running counts its lost attempt.
    let t3 = rec.tasks.iter().find(|t| t.id == TaskId(3)).unwrap();
    assert_eq!(t3.attempt, 1);
    assert!(rec.terminal.is_empty());
    assert_eq!(store.prune(Duration::from_secs(3600)).await.unwrap(), 1);
    assert!(
        store
            .get_task(&TaskRef::Key("ancient".into()))
            .await
            .unwrap()
            .is_none()
    );
}

/// A second instance waits while the first leads, gets a higher epoch once it is released,
/// and the first is fenced from then on, even through a fresh instance.
pub async fn second_instance_waits_and_fences_the_first<H: Harness>(h: &H) {
    let a = h.open().await;
    let b = h.open().await;
    let ea = a.acquire_leadership().await.unwrap();
    a.commit(
        ea,
        BASE,
        &[submitted(1, 0, TaskSpec::new(Bytes::new()))],
        &[],
    )
    .await
    .unwrap();

    let b2 = b.clone();
    let waiting = tokio::spawn(async move { b2.acquire_leadership().await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished(), "leadership is held by the first");

    a.release_leadership().await;
    let eb = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(eb > ea);
    assert!(b.is_leader() && !a.is_leader());
    let late = [submitted(2, 1, TaskSpec::new(Bytes::new()))];
    let r = a.commit(ea, BASE, &late, &[]).await;
    assert!(matches!(r, Err(StoreError::Fenced)), "{r:?}");
    b.commit(eb, BASE, &late, &[]).await.unwrap();
    let c = h.open().await;
    let r = c.commit(ea, BASE, &[], &[]).await;
    assert!(matches!(r, Err(StoreError::Fenced)), "{r:?}");
}

/// Releasing through the trait object hands leadership to a waiting instance.
pub async fn release_lets_a_standby_lead<H: Harness>(h: &H) {
    let a = h.open().await;
    let b = h.open().await;
    let dyn_a: Arc<dyn Store<Bytes, Bytes>> = Arc::new(a.clone());
    dyn_a.acquire_leadership().await.unwrap();
    dyn_a.release_leadership().await;
    assert!(!a.is_leader());
    tokio::time::timeout(Duration::from_secs(5), b.acquire_leadership())
        .await
        .expect("leadership released")
        .unwrap();
}

/// A key released and reused within one batch: [Status(A, Succeeded), Submitted(B, key)].
pub async fn key_reuse_in_same_batch<H: Harness>(h: &H) {
    let store = h.open().await;
    let e = store.acquire_leadership().await.unwrap();
    let cfg = CoreConfig {
        persist: true,
        retention: Duration::ZERO,
        ..Default::default()
    };
    let mut core: Core<Bytes> = Core::new(cfg);
    core.set_capacity(Time::ZERO, PoolId(0), 1);
    let mut acts = Vec::new();
    let a = core
        .submit(Time(S), TaskSpec::new(Bytes::from("a")).key("k"))
        .unwrap();
    core.tick(Time(S));
    core.drain_actions(&mut acts);
    let mut lease = None;
    let mut b1 = Vec::new();
    for x in acts.drain(..) {
        match x {
            Action::Persist(m) => b1.push(*m),
            Action::Dispatch(d) => lease = Some(d.lease),
            _ => {}
        }
    }
    store.commit(e, BASE, &b1, &[]).await.unwrap();
    core.complete(Time(2 * S), lease.unwrap(), Outcome::Success);
    let b = core
        .submit(Time(2 * S + 1), TaskSpec::new(Bytes::from("b")).key("k"))
        .unwrap();
    assert!(!b.duplicate && b.id != a.id);
    core.drain_actions(&mut acts);
    let b2: Vec<_> = acts
        .drain(..)
        .filter_map(|x| match x {
            Action::Persist(m) => Some(*m),
            _ => None,
        })
        .collect();
    store.commit(e, BASE, &b2, &[]).await.unwrap();
    let t = store
        .get_task(&TaskRef::Key("k".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.id, b.id);
    assert!(!t.status.is_some_and(|s| s.is_terminal()));
}

/// Children of parents that finished outside the retention window still run after
/// failover, and pruning keeps parents that live tasks depend on.
pub async fn deps_on_parents_outside_retention_survive_failover<H: Harness>(h: &H) {
    let store = h.open().await;
    let e = store.acquire_leadership().await.unwrap();
    let t0 = hours_ago(2);
    let batch = vec![
        submitted_at(1, 0, t0, TaskSpec::new(Bytes::from("p")).key("p")),
        status(1, TaskStatus::Running, 1, t0 + S, None),
        status(1, TaskStatus::Failed, 1, t0 + 2 * S, Some("boom")),
        submitted_at(2, 1, t0, TaskSpec::new(Bytes::from("q")).key("q")),
        submitted_at(
            3,
            2,
            t0,
            TaskSpec::new(Bytes::from("c1"))
                .key("c1")
                .dep(Dep::on_failure(TaskId(1)))
                .dep(Dep::on_success("q")),
        ),
        submitted_at(
            4,
            3,
            t0,
            TaskSpec::new(Bytes::from("c2"))
                .key("c2")
                .dep(Dep::on_complete("p"))
                .dep(Dep::on_success("q")),
        ),
    ];
    store.commit(e, BASE, &batch, &[]).await.unwrap();
    store.release_leadership().await;
    assert_eq!(store.prune(Duration::from_secs(3600)).await.unwrap(), 0);

    let s = scheduler(h.open().await, Runs::default(), Duration::from_millis(20)).await;
    until_status(&s, "c1", TaskStatus::Succeeded).await;
    until_status(&s, "c2", TaskStatus::Succeeded).await;
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

/// A fixed start and recurring occurrences missed hours ago (on_miss = Skip) do not run after
/// a restart.
pub async fn long_missed_starts_are_not_run_after_restart<H: Harness>(h: &H) {
    let store = h.open().await;
    let e = store.acquire_leadership().await.unwrap();
    let t0 = hours_ago(2);
    let spec = TaskSpec::new(Bytes::from("fx"))
        .key("fx")
        .fixed_start(FixedStart {
            at: Time(t0 + 60 * S),
            tolerance: Duration::from_secs(5),
            on_miss: MissPolicy::Skip,
        });
    store
        .commit(e, BASE, &[submitted_at(1, 0, t0, spec)], &[])
        .await
        .unwrap();
    let recurring = |name: &str, cron: &str| Mutation::RecurringUpsert {
        spec: RecurringSpec {
            name: name.into(),
            cron: cron.into(),
            tz: None,
            template: TaskSpec::new(Bytes::from(name.to_string())),
            deadline_after: None,
            overlap: OverlapPolicy::Skip,
            tolerance: Duration::from_secs(5),
            on_miss: MissPolicy::Skip,
        },
    };
    let next = |name: &str| Mutation::RecurringNext {
        name: name.into(),
        next: Some(Time(t0 + 120 * S)),
        last: None,
    };
    let batch = [
        recurring("hourly", "0 * * * *"),
        next("hourly"),
        recurring("yearly", "0 0 1 1 *"),
        next("yearly"),
    ];
    store.commit(e, BASE, &batch, &[]).await.unwrap();
    store.release_leadership().await;

    let runs = Runs::default();
    let s = scheduler(h.open().await, runs.clone(), Duration::from_millis(20)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_ne!(s.status("fx").await.unwrap(), Some(TaskStatus::Succeeded));
    let runs = runs.lock().unwrap().clone();
    assert!(runs.is_empty(), "missed work ran: {runs:?}");
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

/// The stored message and attempt do not depend on how mutations were batched.
pub async fn message_does_not_depend_on_batching<H: Harness>(h: &H) {
    let store = h.open().await;
    let e = store.acquire_leadership().await.unwrap();
    let t = now();
    let seq = |id| {
        vec![
            status(id, TaskStatus::Running, 1, t, None),
            status(id, TaskStatus::Retrying, 1, t + S, Some("transient error")),
            status(id, TaskStatus::Running, 2, t + 2 * S, None),
        ]
    };
    let subs = [
        submitted(1, 0, TaskSpec::new(Bytes::new())),
        submitted(2, 1, TaskSpec::new(Bytes::new())),
    ];
    store.commit(e, BASE, &subs, &[]).await.unwrap();
    store.commit(e, BASE, &seq(1), &[]).await.unwrap();
    for m in seq(2) {
        store.commit(e, BASE, &[m], &[]).await.unwrap();
    }
    let mut all = vec![submitted(3, 2, TaskSpec::new(Bytes::new()))];
    all.extend(seq(3));
    store.commit(e, BASE, &all, &[]).await.unwrap();
    let get = |id| {
        let store = store.clone();
        async move {
            store
                .get_task(&TaskRef::Id(TaskId(id)))
                .await
                .unwrap()
                .unwrap()
        }
    };
    let (m1, m2, m3) = (get(1).await, get(2).await, get(3).await);
    assert_eq!(m1.message.as_deref(), Some("transient error"));
    assert_eq!((m1.message, m1.attempt), (m2.message, m2.attempt));
    assert_eq!(
        (m3.message.as_deref(), m3.attempt),
        (Some("transient error"), 2)
    );
    assert_eq!(m3.status, Some(TaskStatus::Running));
}

/// Huge durations, large ids, non-ASCII tags and a duplicate submission in one batch.
pub async fn codec_edges<H: Harness>(h: &H) {
    let store = h.open().await;
    let e = store.acquire_leadership().await.unwrap();
    let big = Duration::from_secs(1 << 40);
    let spec = TaskSpec::new(Bytes::new())
        .dep(Dep::on_success(TaskId((32767u64 << 48) | 12345)))
        .tag("ключ", "值 🚀")
        .timeout(Duration::MAX)
        .retry(RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            multiplier: 2.0,
            max_backoff: big,
            jitter: true,
        });
    let batch = [submitted(1, 0, spec.clone()), submitted(1, 0, spec.clone())];
    store.commit(e, BASE, &batch, &[]).await.unwrap();
    let rec = store.recover(BASE).await.unwrap();
    assert_eq!(rec.tasks.len(), 1);
    let t = &rec.tasks[0];
    assert_eq!(t.spec.deps, spec.deps);
    assert_eq!(t.spec.tags, spec.tags);
    assert_eq!(
        t.spec.retry.unwrap().max_backoff,
        big,
        "max_backoff truncated"
    );
    // Durations saturate rather than wrap.
    assert!(t.spec.timeout.unwrap() > Duration::from_secs(200 * 365 * 86400));
}

/// Outputs of finished tasks are served from the store, also to a new leader.
pub async fn outputs_are_loaded_from_the_store<H: Harness>(h: &H) {
    let store = h.open().await;
    let e = store.acquire_leadership().await.unwrap();
    let out = Bytes::from_static(b"answer");
    let batch = [
        submitted(1, 0, TaskSpec::new(Bytes::new()).key("done")),
        status(1, TaskStatus::Succeeded, 1, now(), None),
    ];
    store
        .commit(e, BASE, &batch, &[(TaskId(1), &out)])
        .await
        .unwrap();
    assert_eq!(
        store.load_output(TaskId(1)).await.unwrap(),
        Some(out.clone())
    );
    assert_eq!(store.load_output(TaskId(2)).await.unwrap(), None);
    store.release_leadership().await;

    let s = scheduler(h.open().await, Runs::default(), Duration::from_millis(20)).await;
    let handle = s.handle("done").await.unwrap().expect("known task");
    assert_eq!(*handle.await.unwrap(), out);
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

/// Pruning the newest rows does not let the next leader reissue their ids.
pub async fn ids_are_not_reused_after_prune<H: Harness>(h: &H) {
    let store = h.open().await;
    let e = store.acquire_leadership().await.unwrap();
    let p = (e as u64) << 48;
    let old = hours_ago(3);
    let mut batch = Vec::new();
    for i in 1..=4 {
        batch.push(submitted_at(
            p | i,
            i,
            old,
            TaskSpec::new(Bytes::from(format!("old{i}"))),
        ));
        batch.push(status(p | i, TaskStatus::Succeeded, 1, old + S, None));
    }
    store.commit(e, BASE, &batch, &[]).await.unwrap();
    store.release_leadership().await;
    assert_eq!(store.prune(Duration::from_secs(3600)).await.unwrap(), 4);

    let store2 = h.open().await;
    let s = scheduler(store2.clone(), Runs::default(), Duration::from_millis(20)).await;
    let fresh = s.enqueue(Task::new(Bytes::from("new"))).await.unwrap();
    assert!(
        fresh.id.0 > (p | 4),
        "id {:#x} was already issued",
        fresh.id.0
    );
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}

/// A restart resumes queued and interrupted work and does not rerun finished work.
pub async fn scheduler_resumes_after_restart_without_rerunning_finished_work<H: Harness>(h: &H) {
    let runs = Runs::default();
    let store1 = h.open().await;
    let s1 = Scheduler::<Bytes, Bytes>::builder()
        .pool(Pool::fixed("default", 1).executor_fn({
            let runs = runs.clone();
            move |ctx, p: Arc<Bytes>| pool_step(runs.clone(), ctx, p, 100)
        }))
        .store(store1.clone())
        .build()
        .await
        .unwrap();
    let a = s1
        .submit(Task::new(Bytes::from("a")).key("a"))
        .await
        .unwrap();
    for p in ["b", "c", "d"] {
        s1.enqueue(Task::new(Bytes::from(p)).key(p).after("a"))
            .await
            .unwrap();
    }
    assert_eq!(*a.await.unwrap(), Bytes::from("out:a"));
    tokio::time::sleep(Duration::from_millis(30)).await; // b is now running
    s1.shutdown(Duration::ZERO).await.unwrap(); // abrupt: b aborted, c and d queued
    drop(s1);
    store1.release_leadership().await;

    let store2 = h.open().await;
    let s2 = Scheduler::<Bytes, Bytes>::builder()
        .pool(Pool::fixed("default", 1).executor_fn({
            let runs = runs.clone();
            move |ctx, p: Arc<Bytes>| pool_step(runs.clone(), ctx, p, 10)
        }))
        .store(store2.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(s2.status("a").await.unwrap(), Some(TaskStatus::Succeeded));
    for p in ["b", "c", "d"] {
        until_status(&s2, p, TaskStatus::Succeeded).await;
    }
    let counts = runs.lock().unwrap().clone();
    assert_eq!(counts["a"], 1, "finished work is not re-run");
    for p in ["b", "c", "d"] {
        assert_eq!(counts[p], 1, "{p} completed exactly once ({counts:?})");
        let r = store2
            .get_task(&TaskRef::Key(p.into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.status, Some(TaskStatus::Succeeded));
        assert_eq!(r.output, Some(Bytes::from(format!("out:{p}"))));
    }
    // b's aborted first attempt was persisted as running, so its successful run is attempt 2.
    let b = store2
        .get_task(&TaskRef::Key("b".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b.attempt, 2);
    let epoch2 = s2.enqueue(Task::new(Bytes::from("e"))).await.unwrap().id.0 >> 48;
    assert!(epoch2 >= 2, "new ids carry the new epoch");
    s2.shutdown(Duration::from_secs(1)).await.unwrap();
}

async fn pool_step(
    runs: Runs,
    _ctx: cronoq::TaskContext,
    p: Arc<Bytes>,
    delay_ms: u64,
) -> Result<Bytes, TaskError> {
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    *runs
        .lock()
        .unwrap()
        .entry(String::from_utf8_lossy(&p).into_owned())
        .or_default() += 1;
    Ok(Bytes::from(format!("out:{}", String::from_utf8_lossy(&p))))
}
