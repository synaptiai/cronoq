# Detecting conflicting submissions

A [key](dependencies.md#keys) makes a submission safe to retry: sending it again returns the
task that already exists. That is right when the second submission *is* a retry. It is wrong
when the same key arrives with different content: a producer bug reuses an operation id, two
devices save different text under one edit id, a race builds the key before the payload is
final. By default the second submission is answered like a retry, and the different content is
never run and never reported.

With `on_key_conflict = reject`, cronoq tells the two apart.

| The key | The submission asks for | Result |
|---|---|---|
| is unused | anything | A task is created. |
| belongs to a task | the same thing | The existing task is returned (`duplicate = true`), as for any retry. |
| belongs to a task | something else | **Key conflict.** The submission is refused with the existing task's id and status. The task is unchanged. |

"The same thing" is decided by a **fingerprint**: 128 bits stored with the task when it is
submitted, and compared with the fingerprint of the new submission.

```sh
curl -s $CRONOQ/v1/tasks -H "$AUTH" -H 'content-type: application/json' -d '{
  "key": "edit:8842", "payload": {"text": "second draft"}, "on_key_conflict": "reject"}'
# HTTP 422
# {"code":"unprocessable_entity","error":"the key belongs to task 17 (running), which was
#  submitted with different content","task_id":"17","task_status":"running"}
```

## What the fingerprint covers

The server computes the fingerprint from the request.

| Covered | Not covered |
|---|---|
| Payload bytes | Deadline and fixed start: a retry sent later resolves relative times differently |
| Content type | Tags: they carry things that differ per attempt to submit, such as trace context |
| Pool | Timeout, retry policy, duration hint |
| Kind | Resources and worker labels |
| Priority | Tenant |
| Dependencies | |

An [ordering key](ordering-keys.md) is compared on its own, not through the fingerprint: a
submission whose key is held by an unfinished task of another ordering key is a conflict.

A retry that only differs in the right-hand column is still a retry. Use
[replace by key](replacing.md) when a key is *meant* to take new content.

The fingerprint is taken over the request as it was sent:

- **Payload bytes, not their meaning.** Two JSON encodings of one value (other key order,
  other spacing) are different bytes. A retry through the same client sends the same bytes; a
  producer that submits one key through two different clients should serialise the payload
  itself. The HTTP API re-encodes a JSON `payload` in one canonical form, so key order and
  spacing in an HTTP request body do not matter.
- **Defaults count as what they stand for**: no pool or kind is `default`, no priority is
  normal, no content type is `application/octet-stream`.
- **Dependencies as written**, in any order: a parent named by key and the same parent named
  by id are different references.

In-process callers (the Rust and Python libraries) pass the fingerprint themselves, since the
engine never looks inside a payload: any 128-bit value that is equal for equal requests, for
example a digest of the payload bytes, or of the event the task was created from.

```rust
use cronoq::{Error, KeyConflict, SubmitError, Task};

let task = Task::new(edit)
    .key("edit:8842")
    .fingerprint(digest)                       // u128, chosen by the caller
    .on_key_conflict(KeyConflict::Reject);
match sched.enqueue(task).await {
    Ok(sub) => {}                                                     // created, or a retry
    Err(Error::Submit(SubmitError::KeyConflict { id, status })) => {} // quarantine the item
    Err(e) => return Err(e),
}
```

```python
digest = hashlib.blake2b(body, digest_size=16).digest()
try:
    await sched.submit(edit, key="edit:8842", fingerprint=digest, on_key_conflict="reject")
except cronoq.SubmitError as e:
    if e.code != "key_conflict":
        raise
    quarantine(edit, existing=e.task_id, status=e.task_status)
```

## Rules

- **Stored once.** A task keeps the fingerprint it was submitted with. Changing its priority,
  deadline or tags with [an update](updating.md) does not change it, so a retry of the original
  submission still matches afterwards.
- **A replacement brings its own.** A submission that [replaces a task by key](replacing.md)
  stores its fingerprint with the task, or with the successor it becomes. The newest
  submission accepted for a key is the one a later submission is compared with, also while it
  is still stored behind a running task.
- **No fingerprint matches anything.** A task submitted before fingerprints existed, or by an
  in-process caller that passed none, never conflicts; neither does a submission without one.
- **As long as the key.** A finished task still answers for its key, with its fingerprint, for
  the [retention window](dependencies.md#keys). After that the key is free and creates a new
  task.
- **The mode belongs to the submission.** A stored fingerprint is compared only when the *new*
  submission asks (or the default says so). One queue can hold keys that must never change
  content and keys that are replaced on purpose.

## Batches

A batch submitted with `reject` is checked as a whole before anything is admitted. The first
task whose key conflicts fails the batch, the error names its position, and nothing of the
batch is admitted. In-process batches take the mode per task; over the server it is one mode
for the batch.

## The default

What a submission does when it does not choose is a setting: `return_existing` or `reject`.

| Where | Setting |
|---|---|
| Server | `on_key_conflict` under [`[server]`](../reference/configuration.md#server) |
| Rust | `Builder::on_key_conflict` |
| Python | `Scheduler(on_key_conflict=...)` |

The default of that setting is `return_existing` in this release, so nothing changes until you
ask. **It will become `reject` in the next minor release**, announced in the changelog: a key
reused with other content is almost always a bug worth hearing about. Set it explicitly now to
keep either behaviour across the upgrade. A submission can always choose for itself.

## The error

| | |
|---|---|
| Rust | `SubmitError::KeyConflict { id, status }` (`Error::Batch { index, error }` for a batch) |
| Python | `SubmitError` with `code == "key_conflict"`, `task_id`, `task_status` (and `index` for a batch) |
| gRPC | `FAILED_PRECONDITION` with response metadata `cronoq-conflict-task` and `cronoq-conflict-status` (and `cronoq-batch-index`) |
| HTTP | `422` with `task_id` and `task_status` in the body (and `index`) |
| Clients | Python `ClientError.key_conflict`, Go `Error.KeyConflict()`, TypeScript `CronoqError.keyConflict`, each with the task's id and status |

It is not the error a pool-restricted token gets for a key held in another pool
(`ALREADY_EXISTS` / 409, "task key is already in use"): that one says the key is taken and
nothing more. A conflict never describes a task in a pool the caller may not use; such a
caller gets the plain error instead.

Browsers that call the server over gRPC-Web can read the three metadata values: the server
lists them in `access-control-expose-headers`.

The usual reaction to a conflict is to stop and look: log both submissions and set the item
aside. Retrying cannot succeed while the key is held.

## Upgrading

A 0.2.2 server does not know `reject`.

Over gRPC it ignores the option and compares nothing, for a single submission and for a batch.
If the key is free, the task is created. If the key is in use, the existing task is returned,
whatever it was submitted with. An answer from such a server carries no `outcome`. The Python,
Go and TypeScript clients check that and fail the call with `unimplemented`; the `cronoq`
command-line tool exits with an error. A server that forwards the submission to a 0.2.2
[shard](sharding.md) makes the same check and answers `UNIMPLEMENTED`. The error arrives in
both cases: when the key was free, the call has created the task; when the key was in use,
the call has created nothing.

Over HTTP a 0.2.2 server refuses a request that contains `on_key_conflict` (status 400,
unknown field). Nothing is admitted.

Upgrade every server before relying on `reject`.

A server that forwards a submission to another shard sends its own default along, so the
answer does not depend on which server received the call.

## Durability

Fingerprints are stored with tasks and loaded again after a restart or failover, for live and
for finished tasks, so a conflict is found by whichever server leads. Both stores gain a column
through a schema migration that runs when the database is opened.
