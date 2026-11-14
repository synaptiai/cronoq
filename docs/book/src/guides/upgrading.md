# Upgrading

This page describes what to do when you move from cronoq 0.2.2 to the next release. That
release adds ordering keys, replace by key, conflict detection, updates of queued tasks, pause
and resume, releases, and the embedded library. The [changelog](../changelog.md) lists every
change; this page lists what needs an action.

## Summary

| You run | What to do |
|---|---|
| The server with a Postgres or SQLite store | Back up the database. Stop every replica, then start the new version. The schema is migrated when the new version connects. [Details](#store-migrations) |
| Several servers, or shards | Upgrade every server before producers use the new options. [Details](#order-of-upgrading) |
| Remote workers | Upgrade them when you like. Old workers keep working. New worker libraries work with an old server. [Details](#workers-and-releases) |
| Keys on submissions | Set `on_key_conflict` explicitly now. Its default will change. [Details](#the-default-for-key-conflicts) |
| The Rust crates | Check the breaking changes. [Details](#rust-breaking-changes) |
| A custom `Store` | Handle six new kinds of `Mutation`. [Details](#custom-stores) |
| Tokens pinned to a tenant | They can cancel tasks of their own tenant only. [Details](#other-changes-in-behaviour) |

## Store migrations

Both durable stores record a schema version in the table `cronoq_schema_version`. The new
release moves it forward:

| Store | Version in 0.2.2 | Version in the new release |
|---|---|---|
| Postgres | 5 | 17 |
| SQLite | 1 | 6 |

The migrations only add tables and columns:

| Added for | Postgres version | SQLite version | What is added |
|---|---|---|---|
| Replace by key, conflict detection | 6 to 8 | 2, 3 | Table `cronoq_pending` for submissions stored to run after a started task. Column `fingerprint` on tasks and on that table. |
| Ordering keys | 9 to 13 | 4 | Columns `ordering_key` and `halt_on_failure` on tasks and on `cronoq_pending`. Table `cronoq_ordering` for keys with a floor or a halt. |
| Pauses | 14 | 5 | Table `cronoq_paused`. |
| Releases | 15 to 17 | 6 | Columns `releases` and `max_releases` on tasks, and `max_releases` on `cronoq_pending`. |

Updates of queued tasks need no migration.

**The migrations run by themselves.** The server, or a program that embeds the store, applies
the missing migrations when it opens the database. With several instances, the one that
connects first applies them, under a lock.

**Upgrades roll forward only.** A build refuses to open a database whose schema version is
higher than the one it supports. The error is "database schema version N is newer than this
build supports". There is no migration back to an older version. Once the new release has
opened a database, 0.2.2 cannot use it again.

So:

1. Back up the database before the upgrade. For SQLite, use an online backup
   (`sqlite3 state.db ".backup backup.db"`), not a copy of a file that is in use.
2. Stop every replica of the old version.
3. Start the new version.

Accepted work survives the upgrade: the tasks in the store are loaded by the new leader, and
workers reconnect by themselves.

Do not mix versions on one database. The first new instance that connects migrates the
schema. From then on, an instance of the old version that starts or restarts refuses to run.

## Order of upgrading

Upgrade in this order:

1. **Servers.** All of them, including every [shard](../concepts/sharding.md).
2. **Workers and clients**, in any order.
3. **Producers start to use the new options.**

The reason is how a 0.2.2 server treats options it does not know. Over gRPC it ignores them.
An answer from such a server carries no `outcome`. The Python, Go and TypeScript clients
check that and fail the call with `unimplemented`. The command-line tool exits with an error.
In both cases the old server has already acted on the submission:

| Option sent over gRPC to a 0.2.2 server | What the old server does | What the caller sees |
|---|---|---|
| `ordering_key` | Ignores it. The task is admitted without an ordering key and runs unordered. | The clients fail with `unimplemented`. The command-line tool exits with an error. |
| `on_key_conflict = reject`, single submission or batch | Ignores it and compares nothing. A key that is free: the task is created. A key that is in use: the existing task is returned, whatever it was submitted with. | The clients fail with `unimplemented` in both cases. The command-line tool exits with an error. |
| `on_key_conflict = replace_queued` | Ignores it. A key that is free: the task is created. A key that is in use: the submission is dropped and the existing task is returned unchanged. | The clients fail with `unimplemented` in both cases. The command-line tool exits with an error. |
| `Release` from a worker | Ignores the message. The lease expires. | The worker libraries do not send it to such a server. See [below](#workers-and-releases). |

So the error does not mean that nothing happened. After it, look the key up to see which task
holds it. Do not rely on ordering keys, on `reject` or on replace by key before every server
is upgraded.

Over HTTP a 0.2.2 server refuses a request body that contains a field it does not know
(`ordering_key`, `on_failure`, `on_key_conflict`, `renew_position`) with status 400. Nothing
is admitted.

The same holds between shards. A new server that forwards a submission to a 0.2.2 shard
checks the answer in the same way for an ordering key, for `reject` and for `replace_queued`
(for the last two, only when a task has a key), and fails the call with `UNIMPLEMENTED`. The
old shard has then acted as the table says.

The gRPC protocol only gained messages, fields and methods; none was removed or renumbered.
A request from an old client, which sets none of the new fields, behaves as before, with the
exceptions listed under [Other changes in behaviour](#other-changes-in-behaviour).

## Workers and releases

The worker protocol has one addition: a worker may send `Release{lease, delay_s}` to give an
attempt back without a verdict
([Releasing an attempt](../concepts/reliability.md#releasing-an-attempt)). The protocol
version did not change.

| Server | Worker | What happens |
|---|---|---|
| New | Old | Works as before. An old worker never sends `Release`. |
| New | New | The server sets `Registered.release_supported`. The worker sends `Release`. |
| Old | New | `Registered.release_supported` is not set. The worker libraries send a retryable failure with the delay instead. This uses one retry of the task. |

A worker written directly against the gRPC protocol must make the same check: send `Release`
only when `Registered.release_supported` is set. An older server ignores the message, and the
lease expires.

`Registered` also has the new field `pool_paused`. It carries the reason when the worker
registers for a pool that is [paused](../concepts/pausing.md) as a whole. The worker libraries
log it as a warning.

## The default for key conflicts

A submission whose key is already in use, and that does not choose a behaviour itself, follows
a setting: `on_key_conflict`, with the values `return_existing` and `reject`
([Detecting conflicting submissions](../concepts/key-conflicts.md#the-default)).

- In this release the default of the setting is `return_existing`. Nothing changes until you
  ask.
- The changelog announces that the default becomes `reject` in the next minor release.

Set the value explicitly now. Then the behaviour stays the same across that upgrade,
whichever value you choose.

| Where | Setting |
|---|---|
| Server | `on_key_conflict` under [`[server]`](../reference/configuration.md#server) |
| Helm chart | `onKeyConflict` |
| Rust | `Builder::on_key_conflict` |
| Python | `Scheduler(on_key_conflict=...)` |

A submission can always choose for itself. One related change is already in effect: the
clients no longer send "return the existing task" when a submission names no
`on_key_conflict`. They leave the field unset, and the server's setting decides. With the
default setting nothing changes.

## Rust breaking changes

Before 1.0, a minor version may contain breaking changes. These affect Rust code that uses the
`cronoq` and `cronoq-core` crates.

**`Clock`.** `cronoq::Clock` is `Clone` and no longer `Copy`. Its clones share one clock.
Write `sched.clock().clone()` where the code copied it.

**Cargo features.** Cron schedules, time-zone data and the multi-threaded runtime of
`build_blocking` are now the features `cron`, `tz` and `rt-multi-thread` of the `cronoq`
crate. They are on by default. A dependency with `default-features = false` must name the
ones it needs:

| Without the feature | Effect |
|---|---|
| `cron` | A recurring schedule is refused. |
| `tz` | Schedules run in UTC only. |
| `rt-multi-thread` | `build_blocking` runs a single-threaded runtime on a thread of its own. |

**Priority and urgency settings are checked.** `Builder::build` returns `Error::Config`, and
the server refuses to start, when a value is negative or not finite, when the base scores are
not strictly ordered, or when an `age_cap` lets a lower class pass freshly submitted high or
critical work. Set `allow_overtaking_high` to accept the last case. The defaults pass
unchanged.

**New fields.** Code that builds these types with a struct literal needs the new fields, or
`..Default::default()` where the type has a default.

| Type | New fields |
|---|---|
| `PriorityConfig` | `allow_overtaking_high` |
| `PoolConfig` | `priority`, `urgency`, `max_releases` |
| `CoreConfig` | `on_key_conflict`, `max_ordering_settings` |
| `TaskSpec` | `fingerprint`, `ordering_key`, `on_failure`, `max_releases` |
| `PoolSnapshot` | `priority`, `urgency`, `ordering`, `pauses`, `paused_tasks` |
| `Totals` | `superseded`, `releases` |
| `StatusEvent` | `priority`, `kind` |
| `Submitted` | `outcome` |
| `Recovered` | `pending`, `ordering`, `paused` |
| `RecoveredTask` | `releases` |
| `RecoveredTerminal` | `fingerprint` |

**New variants.** An exhaustive `match` on these enums needs new arms.

| Enum | New variants |
|---|---|
| `SubmitError` | `KeyConflict`, `ReplaceMismatch` |
| `TaskError` | `Release` |
| `Mutation` | `Updated`, `Replaced`, `PendingReplacement`, `Ordering`, `Released`, `Paused` |

`cronoq::Error` also has new variants (`Batch`, `Update`, `Ordering`). It is marked
`#[non_exhaustive]`, so a `match` on it already has a wildcard arm and needs no change.

**Batch errors.** A batch that the engine refuses fails with `Error::Batch { index, error }`
instead of `Error::Submit(error)`.

**Replace with `renew_position`** can fail with `SubmitError::Cycle`, when the task would move
behind a task of its ordering key that waits for it.

## Custom stores

A custom implementation of `cronoq::Store` must handle the new kinds of `Mutation`. The
Postgres and SQLite stores no longer use a catch-all arm, and the store conformance tests
cover each kind, so a missing kind is a compile error there.

| `Mutation` | The store must |
|---|---|
| `Updated` | Set the task's priority, deadline and tags. Otherwise updates are lost on restart. |
| `Replaced` | Apply the replacement to the task. |
| `PendingReplacement` | Keep the stored submission. Drop it when its task reaches a terminal status. Return stored submissions from `recover`. |
| `Ordering` | Store the whole state of one ordering key. All default means: forget the key. Return the stored keys from `recover`. |
| `Paused` | Apply the pause. Return the stored pauses from `recover`. |
| `Released` | Record status `retrying`, the attempt count and the number of releases. Return `RecoveredTask::releases`. |

The store must also keep, and return from `recover`:

- `TaskSpec::fingerprint`, for live and for finished tasks;
- a task's ordering key and its failure policy.

## Other changes in behaviour

| Change | Who is affected |
|---|---|
| Credentials pinned to a tenant can cancel that tenant's tasks only. A `Cancel` of another tenant's task, or of a task without a tenant, answers `NOT_FOUND` (404). Reading is unchanged. | Clients that use tenant-pinned tokens to cancel other tasks. |
| The pool's ready and backlog figures, and the autoscaler's queue pressure, do not count tasks that wait behind another task of their ordering key. | Dashboards and autoscaling, once ordering keys are in use. |
| The engine records the tenant of every task, not only in pools with fair sharing. `TaskSummary::tenant` is set for all of them. | Readers of task listings. |

## The embedded library

The embedded library (`cronoq_ffi`, with the Go and Swift wrappers) is new in this release,
so there is nothing to upgrade from. It is experimental until cronoq 1.0. Its C API has a
version number, `cronoq_abi_version()`, which must equal the `CRONOQ_ABI_VERSION` of the
header you built with. The wrappers check it for you. See
[Embedding from other languages](embedding.md).
