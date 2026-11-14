/*
 * cronoq: a C API for embedding the scheduler. EXPERIMENTAL until 1.0.
 *
 * Generated from crates/cronoq-ffi/src/lib.rs. Do not edit.
 *
 * Rules of the boundary
 * ---------------------
 * Errors      Every function that can fail returns a cronoq_status. Nothing panics or
 *             aborts across the boundary. cronoq_last_error() has the message of the last
 *             failed call on the calling thread.
 * Strings     Text is UTF-8 and zero-terminated. The library copies what it is given
 *             before the call returns; the caller keeps ownership of its buffers.
 * Ownership   What the library returns is released by the library: a cronoq_scheduler
 *             with cronoq_free, a cronoq_lease with cronoq_lease_free, a cronoq_buf with
 *             cronoq_buf_free. Never pass these to free().
 * Threads     A scheduler may be used from any number of threads at once, with one
 *             exception: cronoq_free needs every other thread to be out of the library
 *             (call cronoq_shutdown first, which wakes waiting threads). The library
 *             runs one thread of its own and never calls back into the host.
 * Versions    cronoq_abi_version() == CRONOQ_ABI_VERSION must hold for the header a
 *             program was built with. The number changes when an existing function,
 *             structure or constant changes; additions do not change it.
 */


#ifndef CRONOQ_H
#define CRONOQ_H

#include <stdint.h>
#include <stddef.h>

// The revision of this API. It goes up when an existing function, structure or constant
// changes in a way that breaks a caller built against the previous header. Additions do not
// change it. A caller compares it with `CRONOQ_ABI_VERSION` of the header it was built with.
#define CRONOQ_ABI_VERSION 1

// The result of a call. Every function that can fail returns one. For anything but
// `CRONOQ_OK`, `CRONOQ_TIMEOUT` and `CRONOQ_LEASE_GONE`, [`cronoq_last_error`] has a
// message for the calling thread.
typedef enum cronoq_status {
  // The call did what it was asked to do.
  CRONOQ_OK = 0,
  // `cronoq_next_lease`: no attempt became ready in time.
  CRONOQ_TIMEOUT = 1,
  // `cronoq_next_lease`: no attempt became ready in time, and the pool is paused. The
  // message is the reason the pool was paused with.
  CRONOQ_PAUSED = 2,
  // The scheduler was shut down.
  CRONOQ_SHUTDOWN = 3,
  // An argument is wrong: a null pointer, text that is not UTF-8, JSON that does not
  // parse, an unknown pool or resource, an option that is not allowed.
  CRONOQ_INVALID_ARGUMENT = 4,
  // The task, or the ordering key, does not exist.
  CRONOQ_NOT_FOUND = 5,
  // The lease is over: the attempt timed out, its task was cancelled, or the lease was
  // already ended. The host should stop working on it.
  CRONOQ_LEASE_GONE = 6,
  // The pool is full (`max_pending`), or a limit of the scheduler is reached.
  CRONOQ_FULL = 7,
  // A live task with this key exists and the submission conflicts with it.
  CRONOQ_CONFLICT = 8,
  // The store failed.
  CRONOQ_STORE = 9,
  // A fault inside the library, including a caught panic. The scheduler may be unusable.
  CRONOQ_INTERNAL = 10,
} cronoq_status;

// A subscription to the status events of a scheduler. Made by [`cronoq_events_open`],
// released by [`cronoq_events_close`].
typedef struct cronoq_events cronoq_events;

// A scheduler. Made by [`cronoq_new`], released by [`cronoq_free`].
typedef struct cronoq_scheduler cronoq_scheduler;

// One attempt of a task, handed to the host by [`cronoq_next_lease`]. The pointers stay
// valid until [`cronoq_lease_free`], also after the lease was completed, failed or released.
typedef struct cronoq_lease {
  // Names the attempt in `cronoq_complete`, `cronoq_fail`, `cronoq_release` and
  // `cronoq_heartbeat`.
  uint64_t lease;
  // The task.
  uint64_t task;
  // 1 for the first attempt. Attempt numbers of a task never repeat.
  uint32_t attempt;
  // The payload the task was submitted with. Null when it is empty.
  const uint8_t *payload;
  // Its length in bytes.
  size_t payload_len;
  // The task's key, or null.
  const char *key;
  // The task's kind (an empty string if it has none).
  const char *kind;
  // A JSON object with the rest: `pool`, `tags`, and, when set, `deadline_unix_ms` and
  // `timeout_ms`.
  const char *info_json;
  // Private to the library.
  void *owner;
} cronoq_lease;

// Bytes the library allocated: JSON text in UTF-8, or the output of a task. `ptr[len]` is
// a zero byte, so text is also a C string. Free it with [`cronoq_buf_free`]; never with
// `free`.
typedef struct cronoq_buf {
  // The bytes, or null when empty.
  uint8_t *ptr;
  // Their number, without the closing zero byte.
  size_t len;
} cronoq_buf;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

// The revision of the API this library implements. See `CRONOQ_ABI_VERSION`.
//
// Thread safety: may be called at any time from any thread.
uint32_t cronoq_abi_version(void);

// The version of the library, for example `0.3.0`. The text is static: do not free it.
//
// Thread safety: may be called at any time from any thread.
const char *cronoq_version(void);

// The message of the last call on this thread that did not return `CRONOQ_OK`. For
// `CRONOQ_PAUSED` it is the reason of the pause. Never null; empty if there is none.
//
// Ownership: the text belongs to the library and is valid until the next call into the
// library on the same thread. Copy it to keep it; do not free it.
//
// Thread safety: each thread has its own message.
const char *cronoq_last_error(void);

// Make a scheduler and start it.
//
// `config_json` is a JSON object, or null or empty for one pool named `default`:
//
// ```json
// {
//   "pools": [{
//     "name": "default",
//     "max_leases": 256,
//     "max_pending": 10000,
//     "overflow": "reject",
//     "retry": {"max_attempts": 3, "initial_backoff_ms": 1000, "multiplier": 2.0,
//               "max_backoff_ms": 60000, "jitter": true},
//     "max_releases": 10,
//     "lease_ttl_ms": 30000,
//     "priority": {"aging_rate": {"low": 0.5}},
//     "urgency": {"window_s": 60}
//   }],
//   "resources": [{"kind": "semaphore", "name": "uplink", "permits": 1},
//                 {"kind": "rate", "name": "relay", "rate": 5.0, "burst": 10}],
//   "priority": {"base": {"critical": 1000}, "allow_overtaking_high": false},
//   "urgency": {"max": 100, "window_s": 300},
//   "store": {"sqlite": "/path/to/queue.db"},
//   "events": "final"
// }
// ```
//
// Every field is optional; an unknown field is an error. `overflow` is `reject` (the
// default), `shed_lowest` or `shed_oldest`. `store.sqlite` needs a library built with the
// SQLite store; without `store`, tasks are kept in memory. `events` is `final` (the default:
// subscriptions get one event when a task ends) or `all` (every change of status).
//
// On `CRONOQ_OK`, `*out` is the scheduler. Release it with `cronoq_free`.
//
// Thread safety: may be called from any thread. The scheduler it returns may be used from
// any number of threads at once.
//
// # Safety
// `config_json` is null or a zero-terminated string. `out` points to writable memory for
// one pointer.
enum cronoq_status cronoq_new(const char *config_json, struct cronoq_scheduler **out);

// Stop the scheduler. Threads waiting in `cronoq_next_lease` return `CRONOQ_SHUTDOWN` at
// once and no new lease is handed out. For up to `grace_ms` the host can still report the
// leases it holds (`cronoq_complete`, `cronoq_fail`, `cronoq_release`); the call returns as
// soon as all are reported. A lease that is still held after that is released: its task
// keeps its place and no retry is used. From then on every call returns
// `CRONOQ_SHUTDOWN`. With a store, tasks that did not finish are kept and run after the
// next start.
//
// The scheduler still has to be released with `cronoq_free`. Stopping twice is allowed.
//
// Thread safety: may be called from any thread, also while other threads are inside calls.
//
// # Safety
// `s` is null or a scheduler from `cronoq_new` that was not freed.
enum cronoq_status cronoq_shutdown(struct cronoq_scheduler *s, uint64_t grace_ms);

// Release a scheduler, stopping it first if `cronoq_shutdown` was not called. Null is
// allowed and does nothing.
//
// Thread safety: no other thread may be inside a call with this scheduler, and none may use
// it afterwards. Call `cronoq_shutdown` first to get waiting threads out.
//
// # Safety
// `s` is null or a scheduler from `cronoq_new` that was not freed, and no other thread uses
// it.
void cronoq_free(struct cronoq_scheduler *s);

// Submit a task.
//
// `payload` is copied; the host keeps its buffer. It may be null when `payload_len` is 0.
//
// `options_json` is null, or a JSON object in which every field is optional and an unknown
// field is an error:
//
// ```json
// {
//   "pool": "default",
//   "kind": "send-event",
//   "key": "conversation-17/title",
//   "priority": "normal",
//   "deadline_ms": 5000,
//   "timeout_ms": 30000,
//   "retry": {"max_attempts": 5},
//   "resources": {"uplink": 1},
//   "tags": {"conversation": "17"},
//   "ordering_key": "conversation-17",
//   "on_failure": "halt",
//   "on_key_conflict": "replace_queued",
//   "fingerprint": "9f86d081884c7d65",
//   "renew_position": false,
//   "max_releases": 10
// }
// ```
//
// `priority` is `critical`, `high`, `normal` or `low`. `deadline_ms` counts from now.
// `on_failure` is `continue` or `halt` (for a task with an ordering key). `on_key_conflict`
// is `return_existing`, `reject` or `replace_queued`. `fingerprint` is 1 to 32 hexadecimal
// digits.
//
// On `CRONOQ_OK`, `*out_task` is the task's id and `*out_outcome` says what happened:
// 0 a task was created, 1 a live task with this key exists and is returned, 2 the queued
// task with this key was replaced, 3 the task replaces a running one when that ends.
// Either pointer may be null.
//
// Returns `CRONOQ_FULL` when the pool is full and `CRONOQ_CONFLICT` when the key is taken
// by a task this submission conflicts with.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `payload` points to `payload_len` readable bytes (or is null
// with length 0). `options_json` is null or a zero-terminated string. `out_task` and
// `out_outcome` are null or writable.
enum cronoq_status cronoq_submit(const struct cronoq_scheduler *s,
                                 const uint8_t *payload,
                                 size_t payload_len,
                                 const char *options_json,
                                 uint64_t *out_task,
                                 int32_t *out_outcome);

// Wait up to `timeout_ms` for an attempt of a task of `pool` (null: `default`) and take it.
//
// The scheduler chooses the task when a thread asks, so the order of the queue is decided
// as late as possible. A pool hands out at most `max_leases` attempts at a time.
//
// - `CRONOQ_OK`: `*out` is the attempt. The host runs it and then calls exactly one of
//   `cronoq_complete`, `cronoq_fail` and `cronoq_release`, and `cronoq_lease_free`.
// - `CRONOQ_TIMEOUT`: nothing became ready in time. Call again.
// - `CRONOQ_PAUSED`: nothing became ready in time and the pool is paused; the message is
//   the reason. Call again: the call waits out its timeout, so this is not a busy loop.
// - `CRONOQ_SHUTDOWN`: the scheduler was shut down. Stop calling.
//
// Thread safety: may be called from any number of threads at once; each call blocks its
// thread. Nothing calls back into the host.
//
// Whatever the result, `*out` may be passed to `cronoq_lease_free` afterwards: it is zeroed
// when no lease was handed out.
//
// # Safety
// `s` is a live scheduler. `pool` is null or a zero-terminated string. `out` points to
// writable memory for one `cronoq_lease`.
enum cronoq_status cronoq_next_lease(const struct cronoq_scheduler *s,
                                     const char *pool,
                                     uint64_t timeout_ms,
                                     struct cronoq_lease *out);

// Release what a lease from `cronoq_next_lease` points to, and clear it. This does not end
// the attempt: call `cronoq_complete`, `cronoq_fail` or `cronoq_release` for that, before
// or after. Null, and a lease that was already freed, are allowed.
//
// Thread safety: one thread at a time per lease.
//
// # Safety
// `lease` is null or points to a lease that `cronoq_next_lease` filled (or that is zeroed).
void cronoq_lease_free(struct cronoq_lease *lease);

// Report that the attempt succeeded. The task is done.
//
// `output` is the task's result: bytes that only the host understands, copied by the call.
// It may be null when `output_len` is 0. `cronoq_wait` hands it to whoever waits for the
// task.
//
// Returns `CRONOQ_LEASE_GONE` if the lease is over (timed out, cancelled, already ended):
// the report is ignored.
//
// Thread safety: may be called from any thread, not only the one that took the lease.
//
// # Safety
// `s` is a live scheduler. `output` points to `output_len` readable bytes (or is null with
// length 0).
enum cronoq_status cronoq_complete(const struct cronoq_scheduler *s,
                                   uint64_t lease,
                                   const uint8_t *output,
                                   size_t output_len);

// Report that the attempt failed. `message` (null allowed) is kept with the task.
//
// With `retryable` not 0 the task is tried again after its backoff, at least
// `retry_after_ms` from now when that is not negative, until its retry policy is used up.
// With `retryable` 0 the task fails for good.
//
// Returns `CRONOQ_LEASE_GONE` if the lease is over: the report is ignored.
//
// Thread safety: may be called from any thread.
//
// # Safety
// `s` is a live scheduler. `message` is null or a zero-terminated string.
enum cronoq_status cronoq_fail(const struct cronoq_scheduler *s,
                               uint64_t lease,
                               const char *message,
                               int32_t retryable,
                               int64_t retry_after_ms);

// Give the attempt back without a verdict: the host cannot run the task now, and nothing is
// wrong with the task. The task keeps its place and is handed out again after `delay_ms`.
// This does not use up a retry, up to the pool's or the task's `max_releases`; beyond that
// it counts as a retryable failure.
//
// Returns `CRONOQ_LEASE_GONE` if the lease is over: the report is ignored.
//
// Thread safety: may be called from any thread.
//
// # Safety
// `s` is a live scheduler.
enum cronoq_status cronoq_release(const struct cronoq_scheduler *s,
                                  uint64_t lease,
                                  uint64_t delay_ms);

// Tell the scheduler the host is still working on a lease. Needed only for pools with
// `lease_ttl_ms`: a lease that is not extended within that time expires, and its task is
// tried again.
//
// Returns `CRONOQ_LEASE_GONE` if the lease is over (timed out, its task was cancelled): the
// host should stop working on it. This is how a long attempt learns of a cancel.
//
// Thread safety: may be called from any thread.
//
// # Safety
// `s` is a live scheduler.
enum cronoq_status cronoq_heartbeat(const struct cronoq_scheduler *s, uint64_t lease);

// Cancel a task: the one with `key` when `key` is not null, else the one with id `task`.
// A queued task is removed. The lease of a running one is over when this returns: its next
// heartbeat or report gets `CRONOQ_LEASE_GONE`.
//
// `*out_cancelled` (null allowed) is 1 if a live task was cancelled, 0 if there was none.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `key` is null or a zero-terminated string. `out_cancelled` is
// null or writable.
enum cronoq_status cronoq_cancel(const struct cronoq_scheduler *s,
                                 uint64_t task,
                                 const char *key,
                                 int32_t *out_cancelled);

// Change a queued task (by `key` when not null, else by id `task`) without losing its place
// or its age. `update_json` is a JSON object:
//
// ```json
// {"priority": "high", "deadline_ms": 2000, "no_deadline": false, "tags": {"a": "b"}}
// ```
//
// Every field is optional: `priority` sets the class, `deadline_ms` a deadline that far
// from now, `no_deadline` removes the deadline, `tags` replaces the tags.
//
// `*out_changed` (null allowed) is 1 if something changed. A task that is running is
// changed for its next attempt. Returns `CRONOQ_NOT_FOUND` if there is no such live task.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `key` is null or a zero-terminated string. `update_json` is a
// zero-terminated string. `out_changed` is null or writable.
enum cronoq_status cronoq_update(const struct cronoq_scheduler *s,
                                 uint64_t task,
                                 const char *key,
                                 const char *update_json,
                                 int32_t *out_changed);

// Set the lowest class the tasks of an ordering key run at, so that one urgent task lifts
// the whole key. `floor` is 0 critical, 1 high, 2 normal, 3 low, or -1 to remove the floor.
// With `persist` not 0 the floor stays when the key has no tasks left.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `pool` is null (`default`) or a zero-terminated string.
// `ordering_key` is a zero-terminated string.
enum cronoq_status cronoq_set_ordering_floor(const struct cronoq_scheduler *s,
                                             const char *pool,
                                             const char *ordering_key,
                                             int32_t floor,
                                             int32_t persist);

// Pause a pool, one class of it, or one ordering key of it: its tasks stay queued and keep
// their place, and none of them is handed out until it is resumed. Attempts that are
// running are not interrupted.
//
// `pool` null is `default`. `priority_class` is 0 critical to 3 low, or -1 for none.
// `ordering_key` is null for none. With neither, the whole pool is paused. `reason` is
// required and is shown wherever the pause is.
//
// `*out_changed` (null allowed) is 1 if this paused something that was not paused, 0 if it
// only replaced the reason.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `pool` and `ordering_key` are null or zero-terminated strings.
// `reason` is a zero-terminated string. `out_changed` is null or writable.
enum cronoq_status cronoq_pause(const struct cronoq_scheduler *s,
                                const char *pool,
                                int32_t priority_class,
                                const char *ordering_key,
                                const char *reason,
                                int32_t *out_changed);

// Resume what `cronoq_pause` paused; the arguments name it the same way. Resuming an
// ordering key also resumes a key that a failed task halted.
//
// `*out_changed` (null allowed) is 1 if something was resumed, 0 if nothing was paused.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `pool` and `ordering_key` are null or zero-terminated strings.
// `out_changed` is null or writable.
enum cronoq_status cronoq_resume(const struct cronoq_scheduler *s,
                                 const char *pool,
                                 int32_t priority_class,
                                 const char *ordering_key,
                                 int32_t *out_changed);

// Wait up to `timeout_ms` for a task to end, and get its result. The task is the one with
// `key` when `key` is not null, else the one with id `task`.
//
// - `CRONOQ_OK`: the task ended. `*out_succeeded` is 1 if it succeeded, and `*out` is then
//   the output that was reported with `cronoq_complete` (empty if there was none).
//   Otherwise `*out_succeeded` is 0 and `*out` is text: the final status and the message,
//   for example `dead_lettered: relay refused`.
// - `CRONOQ_TIMEOUT`: the task has not ended yet. Call again.
// - `CRONOQ_NOT_FOUND`: the scheduler does not know the task, or it ended so long ago that
//   its result is no longer kept. The results of the last 1024 tasks that ended are kept in
//   memory; with a store, older ones are read from it.
// - `CRONOQ_SHUTDOWN`: the scheduler was shut down.
//
// Any number of threads may wait for one task; each gets the result.
//
// Ownership: on `CRONOQ_OK` the host owns `*out` and frees it with `cronoq_buf_free`.
// Whatever the result, `*out` may be passed to `cronoq_buf_free` afterwards.
//
// Thread safety: may be called from any number of threads at once; each call blocks its
// thread.
//
// # Safety
// `s` is a live scheduler. `key` is null or a zero-terminated string. `out_succeeded` is
// null or writable. `out` points to writable memory for one `cronoq_buf`.
enum cronoq_status cronoq_wait(const struct cronoq_scheduler *s,
                               uint64_t task,
                               const char *key,
                               uint64_t timeout_ms,
                               int32_t *out_succeeded,
                               struct cronoq_buf *out);

// Subscribe to the status events of the scheduler: what happens to its tasks from now on.
//
// By default an event is sent when a task ends (`succeeded`, `failed`, `dead_lettered`,
// `cancelled`, ...). With `"events": "all"` in the configuration, every change of status is
// sent (`ready`, `running`, `retrying`, ...), and also updates and replacements.
//
// On `CRONOQ_OK`, `*out` is the subscription. Read it with `cronoq_events_next` and release
// it with `cronoq_events_close`. Each subscription gets every event; several may be open.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `out` points to writable memory for one pointer.
enum cronoq_status cronoq_events_open(const struct cronoq_scheduler *s, struct cronoq_events **out);

// Wait up to `timeout_ms` for the next event of a subscription.
//
// - `CRONOQ_OK`: `*out` is the event as JSON:
//
//   ```json
//   {"task": 7, "key": "k", "pool": "default", "status": "succeeded", "attempt": 1,
//    "priority": "normal", "at_unix_ms": 1767225600000, "message": null,
//    "event": "transition"}
//   ```
//
//   `event` is `transition` (the status changed), `updated`, `replaced` or `successor`.
//   If the host read too slowly and events were dropped, the next call returns
//   `{"lagged": 12}` with the number of events that were lost, and reading goes on with
//   the oldest event that is still kept (the last 4096 are).
// - `CRONOQ_TIMEOUT`: no event in time. Call again.
// - `CRONOQ_SHUTDOWN`: the scheduler was shut down; no more events will come.
//
// Ownership: on `CRONOQ_OK` the host owns `*out` and frees it with `cronoq_buf_free`.
// Whatever the result, `*out` may be passed to `cronoq_buf_free` afterwards.
//
// Thread safety: one thread at a time per subscription (a second caller waits); different
// subscriptions may be read at the same time. Each call blocks its thread.
//
// # Safety
// `events` is null or a subscription from `cronoq_events_open` that was not closed. `out`
// points to writable memory for one `cronoq_buf`.
enum cronoq_status cronoq_events_next(const struct cronoq_events *events,
                                      uint64_t timeout_ms,
                                      struct cronoq_buf *out);

// Release a subscription. Null is allowed and does nothing. A subscription may be closed
// before or after its scheduler is freed.
//
// Thread safety: no other thread may be inside `cronoq_events_next` with this subscription.
//
// # Safety
// `events` is null or a subscription from `cronoq_events_open` that was not closed, and no
// other thread uses it.
void cronoq_events_close(struct cronoq_events *events);

// Move the scheduler's time forward by the time it missed while the machine slept.
//
// The scheduler's clock does not count the time a machine sleeps (on Apple platforms and on
// Linux). After a sleep every timer is late by the time slept: retries after a backoff,
// timeouts, deadlines, lease expiry. Call this when the program becomes active again (on
// iOS: when the app returns to the foreground). Timers that became due in the meantime fire
// at once; they are not spread out.
//
// For a lease that was held across the sleep this means: it times out if its task's timeout
// is shorter than the sleep, and it expires if its pool has a `lease_ttl_ms` shorter than
// the sleep. Both count as a failed attempt. Release leases before the program is
// suspended if you can.
//
// The sleep is measured with a monotonic clock of the operating system that counts during
// sleep, so a change of the wall clock has no effect. On Windows, where the library knows
// no such clock, the wall clock is used, and a wall clock that was set forward is followed.
// The scheduler's time never moves back, and when nothing was missed this does nothing:
// calling it more often than needed is harmless.
//
// `*out_moved_ms` (null allowed) is how far the time moved, in milliseconds.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `out_moved_ms` is null or writable.
enum cronoq_status cronoq_resync_clock(const struct cronoq_scheduler *s, uint64_t *out_moved_ms);

// Move the scheduler's time forward by `by_ms` milliseconds, as if that much time had
// passed: timers that are due within it fire at once. **For tests** of backoff and timeouts
// without waiting. The time cannot be moved back, and it stays ahead of the wall clock:
// the times in events are in the future. Do not use it with a store you keep.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler.
enum cronoq_status cronoq_advance_clock(const struct cronoq_scheduler *s, uint64_t by_ms);

// Describe a live task (by `key` when not null, else by id `task`) as JSON:
//
// ```json
// {"id": 7, "key": "k", "pool": "default", "kind": "", "priority": "normal",
//  "status": "ready", "attempt": 0}
// ```
//
// `status` is one of `blocked`, `scheduled`, `ready`, `running`, `retrying` for a live
// task, or a final one (`succeeded`, `failed`, `dead_lettered`, `cancelled`, ...) for a
// short time after the task ended. Returns `CRONOQ_NOT_FOUND` if the scheduler does not
// know the task (any more).
//
// Ownership: on `CRONOQ_OK` the host owns `*out` and frees it with `cronoq_buf_free`.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `key` is null or a zero-terminated string. `out` points to
// writable memory for one `cronoq_buf`.
enum cronoq_status cronoq_task(const struct cronoq_scheduler *s,
                               uint64_t task,
                               const char *key,
                               struct cronoq_buf *out);

// The state of the scheduler as JSON: per pool the queue lengths per class, what is paused
// and why, the ordering keys, and the totals since start.
//
// ```json
// {"pools": [{"name": "default", "capacity": 2, "busy": 1, "ready": 5,
//             "pending": {"critical": 0, "high": 1, "normal": 4, "low": 0},
//             "paused": null, "paused_classes": {"low": "backfill off"},
//             "paused_tasks": 0,
//             "ordering": {"keys": 3, "waiting": 2, "halted": 0, "paused": 0}}],
//  "live_tasks": 6, "running": 1, "pending": 5,
//  "totals": {"submitted": 9, "dispatched": 4, "succeeded": 3, "failed": 0,
//             "dead_lettered": 0, "cancelled": 0, "shed": 0, "retries": 0, "timeouts": 0,
//             "lease_expiries": 0, "superseded": 0, "releases": 0}}
// ```
//
// Ownership: on `CRONOQ_OK` the host owns `*out` and frees it with `cronoq_buf_free`.
//
// Thread safety: may be called from any number of threads at once.
//
// # Safety
// `s` is a live scheduler. `out` points to writable memory for one `cronoq_buf`.
enum cronoq_status cronoq_snapshot(const struct cronoq_scheduler *s, struct cronoq_buf *out);

// Free bytes the library returned (`cronoq_task`, `cronoq_snapshot`) and clear the buffer.
// Null, and a buffer that was already freed, are allowed.
//
// Thread safety: one thread at a time per buffer.
//
// # Safety
// `b` is null or points to a buffer the library filled (or that is zeroed).
void cronoq_buf_free(struct cronoq_buf *b);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* CRONOQ_H */
