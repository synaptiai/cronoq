# cronoq-core

[![crates.io](https://img.shields.io/crates/v/cronoq-core.svg)](https://crates.io/crates/cronoq-core)
[![docs.rs](https://img.shields.io/docsrs/cronoq-core)](https://docs.rs/cronoq-core)

The sans-IO scheduling engine behind [cronoq](https://crates.io/crates/cronoq). `Core` is a
deterministic state machine with no clock, threads, I/O or `unsafe`: every call takes the current
time, and the engine answers with actions (dispatch, abort, scale, persist, publish).

It implements priority classes with aging, deadline urgency (EDF), fixed-start reservations
with worker holds, cron schedules, DAG dependencies, leases, retries, semaphores and token
buckets, bounded queues with shedding, queue-pressure autoscaling and windowed metrics, with
O(classes + log n) dispatch.

```rust
use cronoq_core::*;

let mut core: Core<&str> = Core::new(CoreConfig::default());
core.submit(Time::ZERO, TaskSpec::new("hello").priority(Priority::High)).unwrap();
let Some(Action::Dispatch(d)) = core.poll_action() else { panic!("expected a dispatch") };
core.complete(Time::from_secs_f64(0.5), d.lease, Outcome::Success);
```

**Use it directly** to build a custom driver (another runtime, a simulator) or to test
scheduling policies under virtual time. Most applications should use the
[`cronoq`](https://crates.io/crates/cronoq) crate instead.

- 📘 [Documentation book](https://synaptiai.github.io/cronoq/)
- 🦀 [API reference](https://docs.rs/cronoq-core)

Licensed under MIT or Apache-2.0, at your option.
