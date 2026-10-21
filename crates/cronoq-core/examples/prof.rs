use cronoq_core::*;
fn main() {
    let mut core: Core<u64> = Core::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1024)],
        ..Default::default()
    });
    let classes = Priority::ALL;
    let mut now = 0u64;
    for i in 0..100_000u64 {
        core.submit(
            Time(now),
            TaskSpec::new(i).priority(classes[(i % 4) as usize]),
        )
        .unwrap();
    }
    let mut leases = Vec::new();
    let mut done = 0u64;
    while done < 300_000 {
        now += 1000;
        while let Some(a) = core.poll_action() {
            if let Action::Dispatch(d) = a {
                leases.push(d.lease);
            }
        }
        for l in leases.drain(..) {
            core.complete(Time(now), l, Outcome::Success);
            core.submit(
                Time(now),
                TaskSpec::new(done).priority(classes[(done % 4) as usize]),
            )
            .unwrap();
            done += 1;
        }
    }
}
