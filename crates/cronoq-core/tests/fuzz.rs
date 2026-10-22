//! Randomized operation fuzzing: submissions with every option, cancellations, capacity changes
//! and snapshots interleaved with time. Invariants are checked after every step (`Sim::drain`),
//! and at the end nothing may be stuck: no waiting tasks, holds, reservations or pending counts.

mod common;

use common::*;
use cronoq_core::*;

struct R(u64);

impl R {
    fn n(&mut self, m: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % m
    }
}

fn run(seed: u64) {
    let mut r = R(seed * 7919 + 1);
    let mut p = PoolConfig::fixed("default", 3);
    p.unknown_dependency = UnknownDependency::Await {
        timeout: Some(secs(5.0)),
    };
    p.lease_ttl = seed.is_multiple_of(2).then(|| secs(3.0));
    p.bounds.max_pending = Some(30);
    p.overflow = [
        OverflowPolicy::Reject,
        OverflowPolicy::ShedLowest,
        OverflowPolicy::ShedOldest,
    ][seed as usize % 3];
    let mut p2 = PoolConfig::new("b").workers(0, 1, 4);
    p2.autoscale.as_mut().unwrap().interval = secs(0.5);
    let mut sim = Sim::new(CoreConfig {
        pools: vec![p, p2],
        resources: vec![
            ResourceConfig::semaphore("s", 2),
            ResourceConfig::token_bucket("tb", 2.0, 3),
        ],
        ..Default::default()
    });
    let mut ids = vec![];
    for _ in 0..300 {
        match r.n(10) {
            0..=4 => {
                let mut s = TaskSpec::new(Job::flaky(r.n(30) as f64 / 10.0, r.n(3) as u32))
                    .priority(Priority::from_index(r.n(4) as usize).unwrap())
                    .pool(if r.n(3) == 0 { "b" } else { "default" });
                if r.n(3) == 0 {
                    s = s.key(format!("k{}", r.n(40)));
                }
                if r.n(3) == 0 {
                    s = s.after(format!("k{}", r.n(40)));
                }
                if r.n(4) == 0 {
                    s = s.deadline(sim.now + secs(r.n(20) as f64));
                }
                if r.n(5) == 0 {
                    s = s.resource(if r.n(2) == 0 { "s" } else { "tb" }, 1 + r.n(2) as u32);
                }
                if r.n(5) == 0 {
                    s = s.timeout(secs(1.0 + r.n(3) as f64));
                }
                if r.n(5) == 0 {
                    s = s.fixed_start(FixedStart {
                        at: sim.now + secs(r.n(10) as f64),
                        tolerance: secs(r.n(3) as f64 / 2.0),
                        on_miss: [MissPolicy::RunLate, MissPolicy::Skip, MissPolicy::Fail]
                            [r.n(3) as usize],
                    });
                }
                if r.n(4) == 0 {
                    s = s.retry(RetryPolicy {
                        initial_backoff: secs(0.2),
                        ..Default::default()
                    });
                }
                if let Ok(x) = sim.try_submit(s) {
                    ids.push(x.id);
                }
            }
            5 => {
                if !ids.is_empty() {
                    let id = ids[r.n(ids.len() as u64) as usize];
                    sim.core.cancel(sim.now, &TaskRef::Id(id));
                    sim.drain();
                }
            }
            6 => {
                let c = r.n(5) as u32;
                sim.core.set_capacity(sim.now, PoolId(0), c);
                sim.drain();
            }
            7 => {
                let _ = sim.core.snapshot(sim.now);
                sim.drain();
            }
            _ => {
                let until = sim.now + secs(r.n(20) as f64 / 10.0);
                sim.run_until(until);
            }
        }
    }
    sim.core.set_capacity(sim.now, PoolId(0), 3);
    sim.drain();
    sim.run_until(sim.now + secs(500.0));
    let stuck: Vec<_> = ids
        .iter()
        .filter_map(|id| sim.status(*id).map(|st| (*id, st)))
        .filter(|(_, st)| !st.is_terminal())
        .collect();
    assert!(stuck.is_empty(), "seed {seed}: stuck tasks {stuck:?}");
    let snap = sim.core.snapshot(sim.now);
    for p in &snap.pools {
        assert_eq!(p.held, 0, "seed {seed}: pool {} holds a slot", p.name);
        assert_eq!(
            p.reservations, 0,
            "seed {seed}: pool {} keeps reservations",
            p.name
        );
        assert_eq!(
            p.pending(),
            0,
            "seed {seed}: pool {} has pending work",
            p.name
        );
    }
    assert_eq!(sim.core.awaited_keys(), 0, "seed {seed}: awaits leaked");
}

#[test]
fn fuzz_operations() {
    let seeds: u64 = std::env::var("CRONOQ_FUZZ_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(150);
    for seed in 1..=seeds {
        run(seed);
    }
}
