# Fair sharing between tenants

When several teams or customers share a pool, one of them submitting a burst of work should not
hold everyone else back. A pool with **fair sharing** splits its dispatches between **tenants**
by weight.

```toml
[[pools]]
name = "llm"
[pools.tenant_weights]   # implies fair sharing; tenants not listed weigh 1
acme = 3
globex = 1
```

```rust,ignore
let mut pool = PoolConfig::new("llm");
pool.fairness = Some(Fairness::weights([("acme", 3.0), ("globex", 1.0)]));
sched.submit(Task::new(payload).pool("llm").tenant("acme")).await?;
```

```python
cronoq.Pool("llm", run, tenant_weights={"acme": 3, "globex": 1})
await sched.submit(payload, pool="llm", tenant="acme")
```

Set `fair_share = true` (Python `fair_share=True`) for equal shares without listing weights.

## How it works

- **Priority comes first.** Fair sharing orders tasks *within* a priority class: a critical task
  still runs before any normal one, whatever its tenant.
- **Shares follow weights.** While several tenants have waiting work in a class, each gets
  dispatches in proportion to its weight. With weights 3 and 1, acme gets three dispatches for
  every one of globex's, even if globex queued all of its work first.
- **No banked credit.** A tenant that had nothing waiting does not catch up later: when it
  submits again, it shares from that moment on. A tenant alone in the pool gets everything.
- **Tasks without a tenant** share as one more tenant with the default weight.
- **Deadlines keep their order.** Tasks with a deadline are ordered by urgency, as in any pool,
  and are not part of the fair share.
- Shares count dispatches, not time: give long and short tasks different pools if their run
  times differ widely.

The scheduler uses start-time fair queuing: each task gets a virtual start tag, the later of the
class's virtual time and its tenant's previous finish tag, and tasks run in tag order. Dispatch
stays logarithmic in the backlog.

## Tenants and credentials

The caller names a task's tenant (`tenant` in the API). To stop a client from spending another
tenant's share, pin the tenant on its credentials:

```toml
[[auth.tokens]]
token_env = "CRONOQ_ACME_TOKEN"
role = "client"
tenant = "acme"   # tasks from this token are acme's; naming another tenant is refused
```

A task submitted with such a token gets its tenant filled in; a task that names another tenant
is refused with `PERMISSION_DENIED`.
