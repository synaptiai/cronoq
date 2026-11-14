//! Randomized deterministic simulation: mixed workloads under virtual time, invariants checked
//! after every step (see `Sim::drain`) and end-to-end properties checked at the end.

mod common;

use std::collections::HashMap;

use common::*;
use cronoq_core::*;

struct R(u64);
impl R {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn f(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        self.f() < p
    }
}

/// Updates that changed a task, over all seeds (coverage check).
static UPDATES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Coverage counters: replacements in place, and replacements stored behind a started task.
static REPLACED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static STORED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Coverage counters for ordering keys: halts that were resumed, and tasks that started
/// behind another task of their key.
static RESUMED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ORDERED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Attempts given back by their executor.
static RELEASED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone)]
struct Planned {
    id: TaskId,
    class: Priority,
    pool: usize,
    arrival: Time,
    plain: bool,
    parents: Vec<TaskId>,
    /// Ordering key, and whether the task still holds the place it was submitted at.
    okey: Option<String>,
    renewed: bool,
}

fn duration(r: &mut R, profile: u64) -> f64 {
    match profile {
        0 => 0.001 + r.f() * 0.01,                // microtasks
        1 => 0.5 + (-r.f().max(1e-9).ln()) * 3.0, // LLM-like heavy tail
        _ => 10.0 + r.f() * 60.0,                 // batch
    }
}

fn run_seed(seed: u64, tasks: usize) -> (Totals, usize) {
    let mut r = R(seed);
    let mut pools = vec![
        PoolConfig::new("fast").workers(1, 2, 8),
        PoolConfig::new("llm").workers(1, 1, 6),
        PoolConfig::fixed("batch", 3),
    ];
    for p in &mut pools {
        p.retry = RetryPolicy {
            max_attempts: 3,
            initial_backoff: secs(0.2),
            ..Default::default()
        };
        if let Some(a) = &mut p.autoscale {
            a.cooldown = secs(2.0);
            a.stabilization_window = secs(5.0);
        }
        p.unknown_dependency = UnknownDependency::Await {
            timeout: Some(secs(120.0)),
        };
    }
    pools[0].bounds.max_pending = Some(20);
    pools[0].overflow = OverflowPolicy::ShedLowest;
    pools[1].lease_ttl = Some(secs(600.0));
    let cfg = CoreConfig {
        pools,
        resources: vec![
            ResourceConfig::token_bucket("api", 20.0, 5),
            ResourceConfig::semaphore("gpu", 2),
        ],
        seed,
        ..Default::default()
    };
    let mut s = Sim::new(cfg);
    let pool_names = ["fast", "llm", "batch"];
    let mut planned: Vec<Planned> = Vec::new();
    let mut keyed: Vec<(String, TaskId)> = Vec::new();
    let mut ended_at: HashMap<TaskId, Time> = HashMap::new();

    let mut clock = 0.0;
    for i in 0..tasks {
        clock += r.f() * 0.2;
        s.run_until(t(clock));
        let pool = r.below(3) as usize;
        let class = Priority::from_index(r.below(4) as usize).unwrap();
        let fail = if r.chance(0.1) {
            1 + r.below(4) as u32
        } else {
            0
        };
        let dur = duration(&mut r, pool as u64);
        let mut job = Job::flaky(dur, fail);
        if r.chance(0.02) {
            job.permanent = true;
        }
        let mut spec = TaskSpec::new(job)
            .pool(pool_names[pool])
            .priority(class)
            .kind(format!("k{}", r.below(4)));
        let mut plain = true;
        let mut parents = Vec::new();
        if r.chance(0.03) {
            spec = spec.timeout(secs(dur * 0.8));
            plain = false;
        }
        if pool == 0 && r.chance(0.01) {
            // Burst into the bounded pool to exercise shedding.
            for _ in 0..30 {
                let _ = s.try_submit(
                    TaskSpec::new(Job::ok(0.5))
                        .pool("fast")
                        .priority(Priority::Low),
                );
            }
        }
        if r.chance(0.15) {
            spec = spec.deadline(t(clock + r.f() * 30.0));
            plain = false;
        }
        // An ordering key: a few per pool, so that keys are busy. Not with a fixed start.
        let fixed = r.chance(0.05);
        let okey = (!fixed && r.chance(0.25)).then(|| format!("o{}", r.below(5)));
        if let Some(k) = &okey {
            spec = spec.ordering_key(k.clone());
            if r.chance(0.3) {
                spec = spec.halt_on_failure();
            }
            plain = false;
        }
        if fixed {
            spec = spec.fixed_start(FixedStart {
                at: t(clock + 1.0 + r.f() * 20.0),
                tolerance: secs(1.0),
                on_miss: [MissPolicy::RunLate, MissPolicy::Skip, MissPolicy::Fail]
                    [r.below(3) as usize],
            });
            plain = false;
        }
        if pool == 1 && r.chance(0.5) {
            spec = spec.resource("api", 1 + r.below(2) as u32);
            plain = false;
        }
        if pool == 2 && r.chance(0.3) {
            spec = spec.resource("gpu", 1);
            plain = false;
        }
        if !keyed.is_empty() && r.chance(0.2) {
            for _ in 0..1 + r.below(3) {
                let (k, pid) = keyed[r.below(keyed.len() as u64) as usize].clone();
                let cond = [
                    EdgeCond::OnSuccess,
                    EdgeCond::OnComplete,
                    EdgeCond::OnFailure,
                ][r.below(3) as usize];
                spec = spec.dep(Dep {
                    target: TaskRef::Key(k.into()),
                    cond,
                });
                parents.push(pid);
            }
            plain = false;
        }
        if r.chance(0.02) {
            // Await a key that will be defined later (or never).
            spec = spec.after(format!("future-{}", i + 1 + r.below(5) as usize));
            plain = false;
        }
        let key = if r.chance(0.3) {
            Some(format!("t{i}"))
        } else if r.chance(0.5) {
            Some(format!("future-{i}"))
        } else {
            None
        };
        if let Some(k) = &key {
            spec = spec.key(k.clone());
        }
        if fail > 0 {
            plain = false;
        }
        match s.try_submit(spec) {
            Ok(sub) if !sub.duplicate => {
                if let Some(k) = key {
                    keyed.push((k, sub.id));
                }
                planned.push(Planned {
                    id: sub.id,
                    class,
                    pool,
                    arrival: s.now,
                    plain,
                    parents,
                    okey,
                    renewed: false,
                });
            }
            Ok(_) => {}
            Err(
                SubmitError::ReservationConflict | SubmitError::Cycle | SubmitError::Overloaded,
            ) => {}
            Err(e) => panic!("seed {seed}: unexpected submit error {e:?}"),
        }
        // Ordering keys: a floor now and then, and halted keys are resumed after a while.
        if r.chance(0.02) {
            let floor = Priority::from_index(r.below(5) as usize);
            let key = format!("o{}", r.below(5));
            let pool = PoolId(r.below(3) as u16);
            s.core
                .set_ordering_floor(s.now, pool, &key, floor, r.chance(0.3))
                .unwrap();
            s.drain();
        }
        // An executor gives a running task back now and then.
        if r.chance(0.03) {
            let leases = s.running_leases();
            if !leases.is_empty() {
                let lease = leases[r.below(leases.len() as u64) as usize];
                RELEASED.fetch_add(
                    s.release(lease, secs(r.f())) as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        }
        // Pauses of a pool, a class or an ordering key; all are resumed with the halts.
        if r.chance(0.02) {
            let pool = PoolId(r.below(3) as u16);
            let target = match r.below(4) {
                0 => PauseTarget::Pool,
                1 => PauseTarget::Ordering(format!("o{}", r.below(5)).into()),
                _ => PauseTarget::Class(Priority::from_index(r.below(4) as usize).unwrap()),
            };
            s.core.pause(s.now, pool, &target, "sim").unwrap();
            s.drain();
        }
        if r.chance(0.03) {
            let n = s.resume_halted();
            RESUMED.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        }
        // Random cancellations and capacity jitter.
        if r.chance(0.01) && !planned.is_empty() {
            let victim = planned[r.below(planned.len() as u64) as usize].id;
            s.core.cancel(s.now, &TaskRef::Id(victim));
            s.drain();
        }
        if r.chance(0.005) {
            s.core.set_capacity(s.now, PoolId(2), 1 + r.below(4) as u32);
            s.drain();
        }
        // Random updates of tasks in any state: a new class, a new deadline or none. The
        // engine's invariants are checked after each (in `drain`). A changed task leaves the
        // FIFO check below, which compares tasks by the class they were submitted with.
        if r.chance(0.03) && !planned.is_empty() {
            let i = r.below(planned.len() as u64) as usize;
            let patch = match r.below(3) {
                0 => TaskPatch::default()
                    .priority(Priority::from_index(r.below(4) as usize).unwrap()),
                1 => TaskPatch::default().deadline(s.now + secs(r.f() * 120.0)),
                _ => TaskPatch::default().no_deadline(),
            };
            match s.core.update(s.now, &TaskRef::Id(planned[i].id), patch) {
                Ok(u) if u.changed => {
                    planned[i].plain = false;
                    UPDATES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(_) | Err(UpdateError::NotFound | UpdateError::FixedStart) => {}
                Err(e) => panic!("seed {seed}: unexpected update error {e:?}"),
            }
            s.drain();
        }
        // Random replacements by key ("latest wins") of tasks in any state. A task that has
        // not started takes the new payload; one that has started keeps its own and is
        // followed by a successor. The payload carries a duration no other task has, so the
        // check below can tell which payload each attempt saw.
        if r.chance(0.05) && !keyed.is_empty() {
            let (key, id) = keyed[r.below(keyed.len() as u64) as usize].clone();
            if let Some(i) = planned.iter().position(|p| p.id == id) {
                let dur = 0.01 + (i as f64) * 1e-6 + r.f() * 1e-7;
                let mut spec = TaskSpec::new(Job::ok(dur))
                    .pool(pool_names[planned[i].pool])
                    .priority(Priority::from_index(r.below(4) as usize).unwrap())
                    .key(key);
                if let Some(k) = &planned[i].okey {
                    spec = spec.ordering_key(k.clone());
                }
                let renew = r.chance(0.5);
                match s.replace(spec, renew) {
                    Ok(sub) => {
                        planned[i].renewed |= renew;
                        match sub.outcome {
                            SubmitOutcome::Replaced => {
                                REPLACED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            SubmitOutcome::Pending => {
                                STORED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            _ => {}
                        }
                        if sub.id == id {
                            planned[i].plain = false;
                        }
                    }
                    Err(SubmitError::ReplaceMismatch(_) | SubmitError::Overloaded) => {}
                    // A renewed place would be behind a task of the ordering key that
                    // waits for this one.
                    Err(SubmitError::Cycle) if renew && planned[i].okey.is_some() => {}
                    Err(e) => panic!("seed {seed}: unexpected replace error {e:?}"),
                }
            }
        }
    }
    s.core.set_capacity(s.now, PoolId(2), 3);
    s.drain();
    // Halted keys are resumed until nothing is left.
    loop {
        s.run_to_completion(20_000.0);
        let n = s.resume_halted();
        RESUMED.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        if n == 0 {
            break;
        }
    }

    // Ordering keys. A successor stands for the task it follows.
    let mut root: HashMap<TaskId, TaskId> = HashMap::new();
    for st in &s.started {
        if let Some((_, v)) = st.tags.iter().find(|(k, _)| k == "cronoq.follows") {
            let prev = TaskId(v.parse().expect("task id"));
            let r0 = *root.get(&prev).unwrap_or(&prev);
            root.insert(st.id, r0);
        }
    }
    let by_id: HashMap<TaskId, usize> =
        planned.iter().enumerate().map(|(i, p)| (p.id, i)).collect();
    // Per pool and key: start, end and task (its index in `planned`) of every attempt.
    type Spans = Vec<(Time, Time, usize)>;
    let mut by_key: HashMap<(usize, &str), Spans> = HashMap::new();
    for st in &s.started {
        let r0 = *root.get(&st.id).unwrap_or(&st.id);
        let Some(&i) = by_id.get(&r0) else { continue };
        let Some(k) = &planned[i].okey else { continue };
        let end = *s
            .attempt_end
            .get(&st.lease)
            .unwrap_or_else(|| panic!("seed {seed}: attempt of {} never ended", st.id));
        by_key
            .entry((planned[i].pool, k.as_str()))
            .or_default()
            .push((st.at, end, i));
    }
    for ((pool, key), spans) in &by_key {
        // Attempts are in start order. No two tasks of a key run at once.
        for w in spans.windows(2) {
            assert!(
                w[0].1 <= w[1].0,
                "seed {seed}: tasks {} and {} of key {key} in pool {pool} overlap",
                planned[w[0].2].id,
                planned[w[1].2].id
            );
        }
        // Tasks that kept their place start in submission order.
        let mut first: Vec<usize> = Vec::new();
        for (_, _, i) in spans {
            if !planned[*i].renewed && !first.contains(i) {
                first.push(*i);
            }
        }
        assert!(
            first.windows(2).all(|w| w[0] < w[1]),
            "seed {seed}: key {key} in pool {pool} started out of submission order: {:?}",
            first.iter().map(|i| planned[*i].id).collect::<Vec<_>>()
        );
        ORDERED.fetch_add(
            first.len().saturating_sub(1) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    // Every attempt of one task saw the same payload: a replacement never changes a task
    // that has started.
    let mut seen: HashMap<TaskId, f64> = HashMap::new();
    for st in &s.started {
        let first = *seen.entry(st.id).or_insert(st.dur);
        assert!(
            first == st.dur,
            "seed {seed}: task {} ran with two payloads ({first} and {})",
            st.id,
            st.dur
        );
    }

    for e in &s.events {
        if e.status.is_terminal() {
            assert!(
                ended_at.insert(e.id, e.at).is_none(),
                "seed {seed}: task {} ended twice",
                e.id
            );
        }
    }
    // Every task reached a terminal state (awaits time out, retries end, fixed starts resolve).
    assert_eq!(s.core.live_tasks(), 0, "seed {seed}: tasks still live");
    for p in &planned {
        assert!(
            ended_at.contains_key(&p.id),
            "seed {seed}: task {} never ended",
            p.id
        );
    }
    // Children never start before their parents ended.
    for p in &planned {
        if let Some(start) = s.start_of(p.id) {
            for parent in &p.parents {
                let pe = ended_at
                    .get(parent)
                    .unwrap_or_else(|| panic!("seed {seed}: parent {parent} has no end"));
                assert!(
                    start >= *pe,
                    "seed {seed}: child {} started before parent {parent} ended",
                    p.id
                );
            }
        }
    }
    // FIFO within (pool, class) for plain tasks: earlier arrival never starts later than a
    // plain task of the same class that arrived after it.
    let first_start: HashMap<TaskId, Time> = {
        let mut m = HashMap::new();
        for st in &s.started {
            m.entry(st.id).or_insert(st.at);
        }
        m
    };
    let plain: Vec<&Planned> = planned
        .iter()
        .filter(|p| p.plain && first_start.contains_key(&p.id))
        .collect();
    for (i, a) in plain.iter().enumerate() {
        for b in plain.iter().skip(i + 1) {
            if a.pool == b.pool && a.class == b.class && a.arrival < b.arrival {
                assert!(
                    first_start[&a.id] <= first_start[&b.id],
                    "seed {seed}: FIFO violated in pool {} class {:?}: {} after {}",
                    a.pool,
                    a.class,
                    a.id,
                    b.id
                );
            }
        }
    }
    (s.core.totals().clone(), s.scales.len())
}

#[test]
fn randomized_workloads() {
    let seeds: u64 = std::env::var("CRONOQ_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);
    let mut agg = Totals::default();
    let mut scales = 0;
    for seed in 0..seeds {
        let (t, sc) = run_seed(seed, 400);
        agg.succeeded += t.succeeded;
        agg.retries += t.retries;
        agg.skipped += t.skipped;
        agg.dead_lettered += t.dead_lettered;
        agg.failed += t.failed;
        agg.cancelled += t.cancelled;
        agg.missed += t.missed;
        agg.shed += t.shed;
        agg.timeouts += t.timeouts;
        scales += sc;
    }
    eprintln!("coverage: {agg:?} scale_actions={scales}");
    // The generator must actually exercise the interesting paths.
    assert!(agg.succeeded > 0 && agg.retries > 0 && agg.skipped > 0 && agg.dead_lettered > 0);
    assert!(agg.failed > 0 && agg.cancelled > 0 && scales > 0 && agg.shed > 0 && agg.timeouts > 0);
    assert!(UPDATES.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(REPLACED.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(STORED.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(RESUMED.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(ORDERED.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(RELEASED.load(std::sync::atomic::Ordering::Relaxed) > 0);
}

#[test]
fn randomized_workload_large() {
    let (t, _) = run_seed(0xdead_beef, 3000);
    eprintln!("large: {t:?}");
}
