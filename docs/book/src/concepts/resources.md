# Resources: semaphores and rate limits

A **resource** is a named, shared limit that tasks request units of. Resources are global to a
scheduler: tasks in any pool can use the same resource.

| Kind | Rust | Python | Server config | Behaviour |
|---|---|---|---|---|
| Semaphore | `ResourceConfig::semaphore("gpu", 4)` | `Resource.semaphore("gpu", 4)` | `kind = "semaphore"`, `permits` | At most `permits` units held at once; units return at the end of each attempt. |
| Token bucket | `ResourceConfig::token_bucket("api", 50.0, 100)` | `Resource.token_bucket("api", rate=50.0, burst=100)` | `kind = "token_bucket"`, `rate`, `burst` | Units are consumed; they refill at `rate` per second up to `burst`. |

Request units on the task: `.resource("gpu", 1)` (Rust), `resources={"gpu": 1}` (Python),
`"resources": {"gpu": 1}` (HTTP).

## Dispatch rules

1. When a task reaches the front of the queue, all its resources are checked together. If every
   one has enough units, they are all taken at once (no partial acquisition, so no deadlocks
   between tasks needing several resources).
2. If any is short, the task is **parked** on that resource and removed from the dispatch queue.
   The next task in the pool is considered immediately, so work that does not need the resource
   is not blocked.
3. When units return (semaphore) or refill (token bucket), parked tasks are re-queued in FIFO
   order, as many as the available units can satisfy. A token bucket schedules a timer for the
   moment enough tokens will have accumulated.
4. A request larger than the resource's capacity (`permits` or `burst`) is rejected at
   submission with `ResourceExceedsCapacity`. Requests for unknown resources fail with
   `UnknownResource`.

## Choosing between them

- Use a **semaphore** for things that are *held* while working: GPUs, connections, licences,
  concurrent requests to a partner.
- Use a **token bucket** for *rates*: requests per second or minute, tokens per minute
  (request one unit per thousand tokens, for example).
- Combine both for APIs with a rate limit *and* a concurrency limit.

## Interaction with priorities

Parking happens after priority selection, so the most urgent task that *can* run is dispatched.
A parked high-priority task gets the resource as soon as it is available, ahead of later
waiters for that resource. Parked tasks are not counted as priority inversions.

## Resources and shards

With [sharding](sharding.md), each shard counts its resources on its own, so every resource
belongs to one shard (`shard = "..."`) and only that shard's tasks may use it. To share an
outside limit between shards, define one resource per shard with its share of the limit.
