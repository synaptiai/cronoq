# Cost-aware autoscaling

## The problem

Fixed fleets are over-provisioned most of the day and saturated at peak. CPU-based autoscaling
reacts to the wrong signal for I/O-bound work: a worker waiting on a model API uses no CPU while
hundreds of tasks queue up behind it. And nothing scales *ahead* of a known event such as a
scheduled job.

## The setup

Each pool computes a **desired capacity** from queue pressure every second:

```rust,ignore
use std::time::Duration;
use cronoq::{AutoscaleConfig, Pool};

let pool = Pool::new("workers")
    .workers(1, 2, 50)                                 // min, initial, max
    .autoscale(AutoscaleConfig {
        target_utilization: 0.75,                      // steady-state headroom
        drain_time: Duration::from_secs(30),           // clear a backlog within 30 s
        slo: [Some(Duration::from_millis(500)),        // critical may wait ≤ 0.5 s
              Some(Duration::from_secs(5)), None, None], // high ≤ 5 s
        stabilization_window: Duration::from_secs(120),// scale down slowly
        cooldown: Duration::from_secs(30),
        cost_per_worker_second: 0.0011,                // e.g. $4/h per worker
        ..Default::default()
    });
```

- **In-process** (Rust/Python library), capacity is a number of concurrent task slots and the
  built-in `SlotScaler` applies it immediately.
- **With the server**, capacity is the slots of connected workers. The desired count is exported
  as `cronoq_pool_desired_workers{pool}` for KEDA/HPA, or posted to a webhook
  (`scaler = { kind = "webhook", url = "..." }`), which then starts or stops workers.

A KEDA `ScaledObject` for a worker Deployment:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: llm-workers }
spec:
  scaleTargetRef: { name: llm-workers }      # Deployment of workers with slots=1
  minReplicaCount: 0
  maxReplicaCount: 50
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus:9090
        query: max(cronoq_pool_desired_workers{pool="llm"})
        threshold: "1"
```

## The policy

```text
base   = ceil(arrival_rate × mean_service_time / target_utilization)
drain  = ceil(backlog × mean_service_time / drain_time)
slo    = smallest capacity keeping predicted wait ≤ SLO for every class with backlog
desired = clamp(max(base + drain, slo, busy) + reservations_in_horizon, min, max)
```

Scale-up happens immediately, limited per step to +100% or +4 workers (whichever is larger).
Scale-down only goes to the highest desired value seen over the stabilization window, by at most
one worker per cooldown. Details: [Pools and autoscaling](../concepts/autoscaling.md).

## What you get

- **Scaling on the right signal**: queue pressure and wait-time objectives, not CPU.
- **Scale-to-zero friendly**: with `min = 0`, idle pools cost nothing; the first arrival raises
  the desired count.
- **Pre-warming for scheduled work**: reservations starting within the horizon (60 s by default)
  add capacity before they start.
- **Stable fleets**: stabilization and cooldown prevent flapping during bursty traffic.
- **Cost reporting**: each pool reports `worker_seconds` and `cost`, so you can compare policies
  in money, not just utilization.
