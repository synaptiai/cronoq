# Idempotent producers

## The problem

A producer submits a task and the call times out. The producer does not know whether the task
was accepted, so it submits again. Without protection the work runs twice.

The usual protection is a [key](../concepts/dependencies.md#keys): a name the producer gives
the task, such as an operation id. A second submission with the key of an existing task
returns that task and creates nothing. This makes a retry safe.

It also hides a class of bugs. Sometimes the same key arrives with *different* content: a
producer reuses an operation id, two devices save different text under one edit id, or the key
is built before the payload is final. A plain key answers this like a retry. The different
content is never run, and nobody is told.

## The setup

Submit with a key and with `on_key_conflict = reject`
([Detecting conflicting submissions](../concepts/key-conflicts.md)). cronoq then tells a retry
from a reuse:

| The key | The submission asks for | Result |
|---|---|---|
| is unused | anything | A task is created. |
| belongs to a task | the same thing | The existing task is returned (`duplicate = true`). This is a retry. |
| belongs to a task | something else | **Key conflict.** The submission is refused with the existing task's id and status. The task is unchanged. |

"The same thing" is decided by a **fingerprint**: 128 bits stored with the task and compared
with the fingerprint of the new submission.

### Over the server

The server computes the fingerprint from the request: payload bytes, content type, pool, kind,
priority and dependencies. The producer only sets the option.

```sh
curl -s $CRONOQ/v1/tasks -H "$AUTH" -H 'content-type: application/json' -d '{
  "key": "edit:8842", "payload": {"text": "second draft"}, "on_key_conflict": "reject"}'
# HTTP 422
# {"code":"unprocessable_entity","error":"the key belongs to task 17 (running), which was
#  submitted with different content","task_id":"17","task_status":"running"}
```

The command-line tool has `cronoq submit ... --key edit:8842 --reject-conflict`.

### In process

The engine never looks inside a payload, so in-process callers pass the fingerprint
themselves: any 128-bit value that is equal for equal requests, for example a digest of the
payload bytes.

```python
digest = hashlib.blake2b(body, digest_size=16).digest()
try:
    await sched.submit(edit, key="edit:8842", fingerprint=digest, on_key_conflict="reject")
except cronoq.SubmitError as e:
    if e.code != "key_conflict":
        raise
    quarantine(edit, existing=e.task_id, status=e.task_status)
```

A submission without a fingerprint never conflicts.

### For every submission

To check every submission that does not choose for itself, set the default: `on_key_conflict`
under `[server]` in the server configuration, `Builder::on_key_conflict` in Rust, or
`Scheduler(on_key_conflict=...)` in Python. The default of that setting is `return_existing`
in this release. The changelog announces that it becomes `reject` in the next minor release.
Set it explicitly to keep either behaviour across that upgrade
([Upgrading](../guides/upgrading.md#the-default-for-key-conflicts)).

## What the producer does with the answer

| Answer | Meaning | Reaction |
|---|---|---|
| A task, `duplicate = false` | The task was created. | Continue. |
| A task, `duplicate = true` | An earlier attempt to submit was accepted. | Continue. Use the returned id. |
| Key conflict (HTTP 422, gRPC `FAILED_PRECONDITION`) | The key is held by a task with other content. | Stop and look. Log both submissions and set the item aside. A retry cannot succeed while the key is held. |

## What you get

- **Safe retries.** A producer can submit again after any timeout. A retry returns the task
  that exists.
- **Reuse is reported, not hidden.** A key that arrives with other content fails with the id
  and the status of the task that holds it.
- **A retry sent later is still a retry.** Deadline, fixed start, tags, timeout, retry policy,
  resources, worker labels and tenant are not part of the fingerprint. A producer may change
  trace tags or resolve a relative deadline again without causing a conflict.
- **Atomic batches.** A batch submitted with `reject` is checked as a whole. If one task
  conflicts, nothing of the batch is admitted, and the error names the position of the task.
- **Works after a failover.** Fingerprints are stored with tasks, for live and for finished
  tasks, so a new leader finds the same conflicts.

## Trade-offs

- **The protection lasts as long as the key.** A finished task answers for its key during the
  retention window (1 hour by default). After that the key is free, and a late retry creates a
  new task.
- **Bytes are compared, not meaning.** Two JSON encodings of one value, with another key order
  or other spacing, are different bytes. A producer that submits one key through two different
  clients should serialise the payload itself. The HTTP API re-encodes a JSON `payload` in one
  canonical form, so key order and spacing in an HTTP request body do not matter.
- **Tasks without a fingerprint never conflict.** This includes tasks that were submitted
  before fingerprints existed, and in-process submissions that pass none.
- **Submission is idempotent, execution is not.** cronoq runs a task at least once. Make the
  handler idempotent as well, or use attempt numbers as fencing tokens
  ([Exactly-once side effects](../concepts/reliability.md#exactly-once-side-effects)).
- **`reject` and `replace_queued` are different choices.** `reject` is for keys that must
  never change content. Use [replace by key](latest-wins.md) when a key is meant to take new
  content. The choice belongs to each submission, so one queue can hold both kinds.
- **Older servers.** Over gRPC a 0.2.2 server ignores `reject` and compares nothing. The
  Python, Go and TypeScript clients detect that, because the answer carries no outcome, and
  fail with `unimplemented`; the command-line tool exits with an error. If the key was free,
  the call has still created the task. If the key was in use, the call has created nothing.
  Over HTTP a 0.2.2 server refuses the unknown field (status 400) and admits nothing. Upgrade
  every server before you rely on `reject` ([Upgrading](../guides/upgrading.md)).
