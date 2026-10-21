# LLM and agent workloads

## The problem

Applications built on large language models send many requests to model APIs: chat turns,
tool calls, embeddings, batch summarization, evaluation runs. These requests:

- share provider **rate limits** (requests or tokens per minute) across every worker;
- fail with **HTTP 429** when the limit is exceeded, often with a `Retry-After` header;
- have **mixed urgency**: a user is waiting for a chat reply, nobody is waiting for tonight's
  evaluation run;
- take **seconds to minutes**, with a long tail;
- run on **expensive workers** (or are billed per concurrent request).

A plain queue sends requests as fast as workers can pop them. During a burst every worker hits
the rate limit, retries immediately, and makes it worse. Meanwhile user-facing requests wait
behind batch work.

## The setup

```rust,ignore
use std::{sync::Arc, time::Duration};
use cronoq::*;

let sched = Scheduler::<Prompt, Answer>::builder()
    // The provider allows 50 requests/s with bursts of 100; every pool shares it.
    .resource(ResourceConfig::token_bucket("openai", 50.0, 100))
    // At most 20 requests in flight at once (connection or concurrency limit).
    .resource(ResourceConfig::semaphore("openai-concurrency", 20))
    .pool(
        Pool::new("llm")
            .workers(2, 4, 64)
            .autoscale(AutoscaleConfig {
                // Interactive requests should not wait more than 2 s in the queue.
                slo: [None, Some(Duration::from_secs(2)), None, None],
                cost_per_worker_second: 0.0004,
                ..Default::default()
            })
            .executor_fn(|ctx, p: Arc<Prompt>| async move {
                match call_model(&p).await {
                    Ok(a) => Ok(a),
                    Err(ApiError::RateLimited { retry_after }) => {
                        Err(TaskError::retry_after("429", retry_after))
                    }
                    Err(ApiError::BadRequest(m)) => Err(TaskError::permanent(m)),
                    Err(e) => Err(TaskError::from_err(e)),
                }
            }),
    )
    .build()
    .await?;

// A chat turn: high priority, must start within 2 seconds.
sched.submit(Task::new(chat_prompt)
    .pool("llm").kind("chat")
    .priority(Priority::High)
    .deadline_in(Duration::from_secs(2))
    .timeout(Duration::from_secs(60))
    .resource("openai", 1).resource("openai-concurrency", 1)).await?;

// An evaluation item: low priority, retried generously.
sched.enqueue(Task::new(eval_prompt)
    .pool("llm").kind("eval")
    .priority(Priority::Low)
    .retry(RetryPolicy { max_attempts: 8, ..Default::default() })
    .resource("openai", 1).resource("openai-concurrency", 1)).await?;
```

The same setup in Python:

```python
limits = [cronoq.Resource.token_bucket("openai", rate=50.0, burst=100),
          cronoq.Resource.semaphore("openai-concurrency", 20)]
pool = cronoq.Pool("llm", call_model, min=2, max=64,
                   autoscale=cronoq.AutoscaleConfig(slo={"high": 2.0}))
async with cronoq.Scheduler([pool], resources=limits) as sched:
    await sched.submit(chat, pool="llm", priority="high", deadline_in=2.0,
                       resources={"openai": 1, "openai-concurrency": 1})
```

The repository has a runnable version: `cargo run -p cronoq --example llm_agents`.

## What you get

- **No 429 storms.** The token bucket is checked *before* dispatch. A task that cannot get a
  token is parked on the resource without occupying a worker, and is released in FIFO order as
  tokens refill. Requests are paced at the provider's rate instead of bursting into it.
- **Correct backoff.** `TaskError::retry_after` (Python: `Retryable(retry_after=...)`) delays
  the next attempt by at least the server's `Retry-After`, combined with exponential backoff and
  jitter from the retry policy.
- **Users first.** Chat turns are `High` and carry a deadline, so they overtake queued
  evaluation work. Low-priority work still progresses: [aging](../concepts/priorities.md#aging)
  lets it overtake a continuous stream of `Normal` work.
- **Capacity that follows demand.** The pool scales on arrival rate and backlog, and the
  per-class SLO adds capacity when predicted queue wait for `High` work exceeds 2 seconds.
- **Bounded attempts.** Per-attempt timeouts abort hung calls; tasks that keep failing end as
  `DeadLettered` instead of retrying forever.
- **Cost visibility.** `cost_per_worker_second` makes each pool report its accumulated cost.

## Variations

- **Several providers or models**: one token bucket per provider; a task can require several
  resources at once (all are acquired atomically).
- **Token-based limits**: request `resource("openai-tpm", estimated_tokens / 1000)` from a bucket
  sized in thousands of tokens.
- **Agent tool calls that depend on each other**: submit them as a batch with keys and `after`
  edges ([Pipelines and DAGs](pipelines.md)).
- **Workers on GPU machines running local models**: use the server and remote workers
  ([Polyglot worker fleets](worker-fleets.md)).
