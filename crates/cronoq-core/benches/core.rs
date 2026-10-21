//! Core engine throughput: submit -> dispatch -> complete cycles.

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use cronoq_core::*;

fn engine(workers: u32) -> Core<u64> {
    Core::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", workers)],
        ..Default::default()
    })
}

fn fill(core: &mut Core<u64>, depth: u64) {
    for i in 0..depth {
        core.submit(
            Time(0),
            TaskSpec::new(i).priority(Priority::ALL[(i % 4) as usize]),
        )
        .unwrap();
    }
}

/// Steady state: a backlog is kept; every completion frees a slot that is refilled.
fn cycles(core: &mut Core<u64>, n: u64, now: &mut u64) {
    let mut leases = Vec::with_capacity(1024);
    let classes = Priority::ALL;
    let mut done = 0;
    while done < n {
        *now += 1_000;
        while let Some(a) = core.poll_action() {
            if let Action::Dispatch(d) = a {
                leases.push(d.lease);
            }
        }
        for l in leases.drain(..) {
            core.complete(Time(*now), l, Outcome::Success);
            core.submit(
                Time(*now),
                TaskSpec::new(done).priority(classes[(done % 4) as usize]),
            )
            .unwrap();
            done += 1;
        }
    }
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("core");
    for (workers, depth) in [(1u32, 1_000u64), (64, 10_000), (1024, 100_000)] {
        g.throughput(Throughput::Elements(1));
        g.bench_function(format!("cycle/w{workers}/backlog{depth}"), |b| {
            b.iter_custom(|iters| {
                let mut core = engine(workers);
                fill(&mut core, depth);
                let mut now = 0;
                let start = Instant::now();
                cycles(&mut core, iters, &mut now);
                black_box(&core);
                start.elapsed()
            })
        });
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(5)).warm_up_time(Duration::from_secs(1));
    targets = bench
}
criterion_main!(benches);
