//! A C API for embedding the cronoq scheduler in a program written in Go, Swift, C or any
//! other language that can call C.
//!
//! **Experimental until 1.0.** [`cronoq_abi_version`] tells a caller which revision of this
//! API a library implements.
//!
//! The scheduler runs inside the library, on one thread it owns. The host submits tasks
//! (opaque bytes) and runs them itself: its threads take one attempt at a time with
//! [`cronoq_next_lease`], do the work, and report the result with [`cronoq_complete`],
//! [`cronoq_fail`] or [`cronoq_release`]. Nothing ever calls back into the host.
//!
//! The rules of the boundary (who owns which pointer, which calls may run at the same time)
//! are in the generated header, `include/cronoq.h`, and in the book under "Embedding from
//! other languages".

#![allow(non_camel_case_types)]
#![warn(missing_docs)]

mod config;
mod pull;

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, RwLock};
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use cronoq::{
    BlockingScheduler, Error, PauseTarget, Priority, Scheduler, SubmitOutcome, TaskError, TaskId,
    TaskRef,
};
use cronoq_core::{OrderingError, SubmitError, UpdateError};
use serde_json::json;

use config::{Config, TaskJson, UpdateJson};
use pull::{Hub, Next, PullExecutor};

/// The revision of this API. It goes up when an existing function, structure or constant
/// changes in a way that breaks a caller built against the previous header. Additions do not
/// change it. A caller compares it with `CRONOQ_ABI_VERSION` of the header it was built with.
pub const CRONOQ_ABI_VERSION: u32 = 1;

/// The result of a call. Every function that can fail returns one. For anything but
/// `CRONOQ_OK`, `CRONOQ_TIMEOUT` and `CRONOQ_LEASE_GONE`, [`cronoq_last_error`] has a
/// message for the calling thread.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum cronoq_status {
    /// The call did what it was asked to do.
    CRONOQ_OK = 0,
    /// `cronoq_next_lease`: no attempt became ready in time.
    CRONOQ_TIMEOUT = 1,
    /// `cronoq_next_lease`: no attempt became ready in time, and the pool is paused. The
    /// message is the reason the pool was paused with.
    CRONOQ_PAUSED = 2,
    /// The scheduler was shut down.
    CRONOQ_SHUTDOWN = 3,
    /// An argument is wrong: a null pointer, text that is not UTF-8, JSON that does not
    /// parse, an unknown pool or resource, an option that is not allowed.
    CRONOQ_INVALID_ARGUMENT = 4,
    /// The task, or the ordering key, does not exist.
    CRONOQ_NOT_FOUND = 5,
    /// The lease is over: the attempt timed out, its task was cancelled, or the lease was
    /// already ended. The host should stop working on it.
    CRONOQ_LEASE_GONE = 6,
    /// The pool is full (`max_pending`), or a limit of the scheduler is reached.
    CRONOQ_FULL = 7,
    /// A live task with this key exists and the submission conflicts with it.
    CRONOQ_CONFLICT = 8,
    /// The store failed.
    CRONOQ_STORE = 9,
    /// A fault inside the library, including a caught panic. The scheduler may be unusable.
    CRONOQ_INTERNAL = 10,
}

use cronoq_status::*;

/// Bytes the library allocated: JSON text in UTF-8. `ptr[len]` is a zero byte, so `ptr` is
/// also a C string. Free it with [`cronoq_buf_free`]; never with `free`.
#[repr(C)]
pub struct cronoq_buf {
    /// The bytes, or null when empty.
    pub ptr: *mut u8,
    /// Their number, without the closing zero byte.
    pub len: usize,
}

/// One attempt of a task, handed to the host by [`cronoq_next_lease`]. The pointers stay
/// valid until [`cronoq_lease_free`], also after the lease was completed, failed or released.
#[repr(C)]
pub struct cronoq_lease {
    /// Names the attempt in `cronoq_complete`, `cronoq_fail`, `cronoq_release` and
    /// `cronoq_heartbeat`.
    pub lease: u64,
    /// The task.
    pub task: u64,
    /// 1 for the first attempt. Attempt numbers of a task never repeat.
    pub attempt: u32,
    /// The payload the task was submitted with. Null when it is empty.
    pub payload: *const u8,
    /// Its length in bytes.
    pub payload_len: usize,
    /// The task's key, or null.
    pub key: *const c_char,
    /// The task's kind (an empty string if it has none).
    pub kind: *const c_char,
    /// A JSON object with the rest: `pool`, `tags`, and, when set, `deadline_unix_ms` and
    /// `timeout_ms`.
    pub info_json: *const c_char,
    /// Private to the library.
    pub owner: *mut c_void,
}

/// What the pointers of a [`cronoq_lease`] point into.
struct LeaseOwner {
    _payload: Arc<Bytes>,
    _key: Option<CString>,
    _kind: CString,
    _info: CString,
}

/// A scheduler. Made by [`cronoq_new`], released by [`cronoq_free`].
pub struct cronoq_scheduler {
    /// `None` after shutdown.
    sched: RwLock<Option<BlockingScheduler<Bytes, Bytes>>>,
    hub: Arc<Hub>,
}

/// A failed call: its status and the message for `cronoq_last_error`.
struct Fail(cronoq_status, String);

type Res<T = ()> = Result<T, Fail>;

fn invalid(msg: impl Into<String>) -> Fail {
    Fail(CRONOQ_INVALID_ARGUMENT, msg.into())
}

impl From<Error> for Fail {
    fn from(e: Error) -> Fail {
        let status = match &e {
            Error::Submit(s) | Error::Batch { error: s, .. } => match s {
                SubmitError::Full | SubmitError::Overloaded => CRONOQ_FULL,
                SubmitError::KeyConflict { .. } => CRONOQ_CONFLICT,
                _ => CRONOQ_INVALID_ARGUMENT,
            },
            Error::Update(UpdateError::NotFound) => CRONOQ_NOT_FOUND,
            Error::Update(_) => CRONOQ_INVALID_ARGUMENT,
            Error::Ordering(OrderingError::TooManySettings) => CRONOQ_FULL,
            Error::Ordering(_) => CRONOQ_INVALID_ARGUMENT,
            Error::Shutdown => CRONOQ_SHUTDOWN,
            Error::Config(_) => CRONOQ_INVALID_ARGUMENT,
            Error::Store(_) => CRONOQ_STORE,
            _ => CRONOQ_INTERNAL,
        };
        Fail(status, e.to_string())
    }
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn set_last_error(msg: &str) {
    // A message with a zero byte inside cannot be a C string: cut it there.
    let msg = msg.split('\0').next().unwrap_or_default();
    let c = CString::new(msg).unwrap_or_default();
    LAST_ERROR.with(|l| *l.borrow_mut() = c);
}

/// Run the body of an entry point: turn its error into a status and a message, and a panic
/// into `CRONOQ_INTERNAL`. No panic leaves the library.
fn guard(f: impl FnOnce() -> Res<cronoq_status>) -> cronoq_status {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(status)) => status,
        Ok(Err(Fail(status, msg))) => {
            set_last_error(&msg);
            status
        }
        Err(p) => {
            let what = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown".into());
            set_last_error(&format!("panic inside cronoq: {what}"));
            CRONOQ_INTERNAL
        }
    }
}

/// Text passed by the host: null is `None`.
///
/// # Safety
/// `p` is null or points to a zero-terminated string that stays valid for `'a`.
unsafe fn text<'a>(name: &str, p: *const c_char) -> Res<Option<&'a str>> {
    if p.is_null() {
        return Ok(None);
    }
    // SAFETY: the caller guarantees a valid C string.
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map(Some)
        .map_err(|_| invalid(format!("{name} is not UTF-8")))
}

/// Text that must be given.
///
/// # Safety
/// As for [`text`].
unsafe fn required<'a>(name: &str, p: *const c_char) -> Res<&'a str> {
    // SAFETY: passed on.
    unsafe { text(name, p) }?.ok_or_else(|| invalid(format!("{name} is null")))
}

/// The pool a call names: null means `default`.
///
/// # Safety
/// As for [`text`].
unsafe fn pool_name<'a>(p: *const c_char) -> Res<&'a str> {
    // SAFETY: passed on.
    Ok(unsafe { text("pool", p) }?.unwrap_or("default"))
}

/// The task a call names: by key when `key` is given, else by id.
///
/// # Safety
/// As for [`text`].
unsafe fn task_ref(id: u64, key: *const c_char) -> Res<TaskRef> {
    // SAFETY: passed on.
    Ok(match unsafe { text("key", key) }? {
        Some(k) => TaskRef::Key(k.into()),
        None => TaskRef::Id(TaskId(id)),
    })
}

/// A class by number: 0 critical, 1 high, 2 normal, 3 low.
fn class(n: i32) -> Res<Priority> {
    usize::try_from(n)
        .ok()
        .and_then(Priority::from_index)
        .ok_or_else(|| invalid(format!("class {n} is not 0 (critical) to 3 (low)")))
}

fn c_string(s: &str) -> CString {
    CString::new(s.replace('\0', "")).unwrap_or_default()
}

fn buf(text: String) -> cronoq_buf {
    let len = text.len();
    let mut bytes = text.into_bytes();
    bytes.push(0);
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
    cronoq_buf { ptr, len }
}

impl cronoq_scheduler {
    /// Run `f` with the scheduler, unless it was shut down.
    fn with<T>(&self, f: impl FnOnce(&BlockingScheduler<Bytes, Bytes>) -> Res<T>) -> Res<T> {
        let guard = self.sched.read().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            Some(b) => f(b),
            None => Err(Fail(CRONOQ_SHUTDOWN, "the scheduler was shut down".into())),
        }
    }
}

/// Bring the capacity of `pool` in line with what the hub wants. Several threads may do this
/// at once; whoever's call is applied last checks again, so the last word is current.
fn sync_capacity(b: &BlockingScheduler<Bytes, Bytes>, hub: &Hub, pool: &str) {
    loop {
        let want = hub.capacity(pool);
        if b.set_capacity(pool, want).is_err() || hub.capacity(pool) == want {
            return;
        }
    }
}

/// The same from the scheduler's own thread, where blocking is not possible.
async fn sync_capacity_async(sched: Scheduler<Bytes, Bytes>, hub: Arc<Hub>, pool: Arc<str>) {
    loop {
        let want = hub.capacity(&pool);
        if sched.set_capacity(&pool, want).await.is_err() || hub.capacity(&pool) == want {
            return;
        }
    }
}

fn build(config: &str) -> Res<cronoq_scheduler> {
    let cfg = Config::parse(config).map_err(invalid)?;
    let priority = cfg.priority();
    let urgency = cfg.urgency().map_err(invalid)?;
    let names: Vec<Arc<str>> = cfg.pools.iter().map(|p| p.name.as_str().into()).collect();
    let hub = Hub::new(names.iter().cloned());
    let mut pools = Vec::new();
    for (p, name) in cfg.pools.iter().zip(&names) {
        let pool = p.pool(&priority, &urgency).map_err(invalid)?;
        pools.push(pool.executor(PullExecutor {
            pool: name.clone(),
            hub: hub.clone(),
        }));
    }
    let resources = cfg.resources();
    let sqlite = cfg.store.as_ref().and_then(|s| s.sqlite.clone());
    #[cfg(not(feature = "sqlite"))]
    if sqlite.is_some() {
        return Err(invalid(
            "store.sqlite: this build of the library has no SQLite store (feature `sqlite`)",
        ));
    }
    let sched = BlockingScheduler::start(move || async move {
        let mut b = Scheduler::<Bytes, Bytes>::builder();
        for p in pools {
            b = b.pool(p);
        }
        for r in resources {
            b = b.resource(r);
        }
        if let Some(p) = priority {
            b = b.priority(p);
        }
        if let Some(u) = urgency {
            b = b.urgency(u);
        }
        #[cfg(feature = "sqlite")]
        if let Some(path) = sqlite {
            b = b.store(cronoq_store_sqlite::SqliteStore::open(path).await?);
        }
        #[cfg(not(feature = "sqlite"))]
        let _ = sqlite;
        Ok(b)
    })?;
    {
        let (sched, hub2) = (sched.scheduler().clone(), Arc::downgrade(&hub));
        hub.on_forget(move |pool| {
            let (Some(hub), Ok(rt)) = (hub2.upgrade(), tokio::runtime::Handle::try_current())
            else {
                return;
            };
            rt.spawn(sync_capacity_async(sched.clone(), hub, pool));
        });
    }
    Ok(cronoq_scheduler {
        sched: RwLock::new(Some(sched)),
        hub,
    })
}

/// The scheduler behind a pointer from the host.
///
/// # Safety
/// `s` is null or a pointer from `cronoq_new` that was not freed.
unsafe fn scheduler<'a>(s: *const cronoq_scheduler) -> Res<&'a cronoq_scheduler> {
    // SAFETY: the caller guarantees a live scheduler or null.
    unsafe { s.as_ref() }.ok_or_else(|| invalid("the scheduler is null"))
}

/// The revision of the API this library implements. See `CRONOQ_ABI_VERSION`.
///
/// Thread safety: may be called at any time from any thread.
#[unsafe(no_mangle)]
pub extern "C" fn cronoq_abi_version() -> u32 {
    CRONOQ_ABI_VERSION
}

/// The version of the library, for example `0.3.0`. The text is static: do not free it.
///
/// Thread safety: may be called at any time from any thread.
#[unsafe(no_mangle)]
pub extern "C" fn cronoq_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// The message of the last call on this thread that did not return `CRONOQ_OK`. For
/// `CRONOQ_PAUSED` it is the reason of the pause. Never null; empty if there is none.
///
/// Ownership: the text belongs to the library and is valid until the next call into the
/// library on the same thread. Copy it to keep it; do not free it.
///
/// Thread safety: each thread has its own message.
#[unsafe(no_mangle)]
pub extern "C" fn cronoq_last_error() -> *const c_char {
    LAST_ERROR.with(|l| l.borrow().as_ptr())
}

/// Make a scheduler and start it.
///
/// `config_json` is a JSON object, or null or empty for one pool named `default`:
///
/// ```json
/// {
///   "pools": [{
///     "name": "default",
///     "max_leases": 256,
///     "max_pending": 10000,
///     "overflow": "reject",
///     "retry": {"max_attempts": 3, "initial_backoff_ms": 1000, "multiplier": 2.0,
///               "max_backoff_ms": 60000, "jitter": true},
///     "max_releases": 10,
///     "lease_ttl_ms": 30000,
///     "priority": {"aging_rate": {"low": 0.5}},
///     "urgency": {"window_s": 60}
///   }],
///   "resources": [{"kind": "semaphore", "name": "uplink", "permits": 1},
///                 {"kind": "rate", "name": "relay", "rate": 5.0, "burst": 10}],
///   "priority": {"base": {"critical": 1000}, "allow_overtaking_high": false},
///   "urgency": {"max": 100, "window_s": 300},
///   "store": {"sqlite": "/path/to/queue.db"}
/// }
/// ```
///
/// Every field is optional; an unknown field is an error. `overflow` is `reject` (the
/// default), `shed_lowest` or `shed_oldest`. `store.sqlite` needs a library built with the
/// SQLite store; without `store`, tasks are kept in memory.
///
/// On `CRONOQ_OK`, `*out` is the scheduler. Release it with `cronoq_free`.
///
/// Thread safety: may be called from any thread. The scheduler it returns may be used from
/// any number of threads at once.
///
/// # Safety
/// `config_json` is null or a zero-terminated string. `out` points to writable memory for
/// one pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_new(
    config_json: *const c_char,
    out: *mut *mut cronoq_scheduler,
) -> cronoq_status {
    guard(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: `out` is writable (caller), `config_json` a C string or null (caller).
        unsafe { *out = std::ptr::null_mut() };
        let cfg = unsafe { text("config_json", config_json) }?.unwrap_or("");
        let s = Box::new(build(cfg)?);
        // SAFETY: as above.
        unsafe { *out = Box::into_raw(s) };
        Ok(CRONOQ_OK)
    })
}

/// Stop the scheduler. Threads waiting in `cronoq_next_lease` return `CRONOQ_SHUTDOWN` at
/// once; every later call returns `CRONOQ_SHUTDOWN`. Leases the host still holds are over.
/// Running attempts get up to `grace_ms` to be reported before the scheduler stops. With a
/// store, tasks that did not finish are kept and run after the next start.
///
/// The scheduler still has to be released with `cronoq_free`. Stopping twice is allowed.
///
/// Thread safety: may be called from any thread, also while other threads are inside calls.
///
/// # Safety
/// `s` is null or a scheduler from `cronoq_new` that was not freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_shutdown(s: *mut cronoq_scheduler, grace_ms: u64) -> cronoq_status {
    guard(|| {
        // SAFETY: caller.
        let s = unsafe { scheduler(s) }?;
        // First wake the threads that wait for a lease: they hold the scheduler.
        s.hub.close();
        let taken = s.sched.write().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(b) = taken {
            b.shutdown(Duration::from_millis(grace_ms))?;
        }
        Ok(CRONOQ_OK)
    })
}

/// Release a scheduler, stopping it first if `cronoq_shutdown` was not called. Null is
/// allowed and does nothing.
///
/// Thread safety: no other thread may be inside a call with this scheduler, and none may use
/// it afterwards. Call `cronoq_shutdown` first to get waiting threads out.
///
/// # Safety
/// `s` is null or a scheduler from `cronoq_new` that was not freed, and no other thread uses
/// it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_free(s: *mut cronoq_scheduler) {
    let _ = guard(|| {
        if s.is_null() {
            return Ok(CRONOQ_OK);
        }
        // SAFETY: the caller hands back a pointer from `cronoq_new` and no longer uses it.
        let s = unsafe { Box::from_raw(s) };
        s.hub.close();
        let taken = s.sched.write().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(b) = taken {
            let _ = b.shutdown(Duration::ZERO);
        }
        Ok(CRONOQ_OK)
    });
}

/// Submit a task.
///
/// `payload` is copied; the host keeps its buffer. It may be null when `payload_len` is 0.
///
/// `options_json` is null, or a JSON object in which every field is optional and an unknown
/// field is an error:
///
/// ```json
/// {
///   "pool": "default",
///   "kind": "send-event",
///   "key": "conversation-17/title",
///   "priority": "normal",
///   "deadline_ms": 5000,
///   "timeout_ms": 30000,
///   "retry": {"max_attempts": 5},
///   "resources": {"uplink": 1},
///   "tags": {"conversation": "17"},
///   "ordering_key": "conversation-17",
///   "on_failure": "halt",
///   "on_key_conflict": "replace_queued",
///   "fingerprint": "9f86d081884c7d65",
///   "renew_position": false,
///   "max_releases": 10
/// }
/// ```
///
/// `priority` is `critical`, `high`, `normal` or `low`. `deadline_ms` counts from now.
/// `on_failure` is `continue` or `halt` (for a task with an ordering key). `on_key_conflict`
/// is `return_existing`, `reject` or `replace_queued`. `fingerprint` is 1 to 32 hexadecimal
/// digits.
///
/// On `CRONOQ_OK`, `*out_task` is the task's id and `*out_outcome` says what happened:
/// 0 a task was created, 1 a live task with this key exists and is returned, 2 the queued
/// task with this key was replaced, 3 the task replaces a running one when that ends.
/// Either pointer may be null.
///
/// Returns `CRONOQ_FULL` when the pool is full and `CRONOQ_CONFLICT` when the key is taken
/// by a task this submission conflicts with.
///
/// Thread safety: may be called from any number of threads at once.
///
/// # Safety
/// `s` is a live scheduler. `payload` points to `payload_len` readable bytes (or is null
/// with length 0). `options_json` is null or a zero-terminated string. `out_task` and
/// `out_outcome` are null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_submit(
    s: *const cronoq_scheduler,
    payload: *const u8,
    payload_len: usize,
    options_json: *const c_char,
    out_task: *mut u64,
    out_outcome: *mut i32,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller, for every pointer below.
        let s = unsafe { scheduler(s) }?;
        let payload = if payload_len == 0 {
            Bytes::new()
        } else if payload.is_null() {
            return Err(invalid("payload is null but payload_len is not 0"));
        } else {
            Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(payload, payload_len) })
        };
        let options =
            TaskJson::parse(unsafe { text("options_json", options_json) }?).map_err(invalid)?;
        let task = options.task(payload).map_err(invalid)?;
        let done = s.with(|b| Ok(b.enqueue(task)?))?;
        if !out_task.is_null() {
            unsafe { *out_task = done.id.0 };
        }
        if !out_outcome.is_null() {
            let n = match done.outcome {
                SubmitOutcome::Created => 0,
                SubmitOutcome::Existing => 1,
                SubmitOutcome::Replaced => 2,
                SubmitOutcome::Pending => 3,
            };
            unsafe { *out_outcome = n };
        }
        Ok(CRONOQ_OK)
    })
}

/// Wait up to `timeout_ms` for an attempt of a task of `pool` (null: `default`) and take it.
///
/// The scheduler chooses the task when a thread asks, so the order of the queue is decided
/// as late as possible. A pool hands out at most `max_leases` attempts at a time.
///
/// - `CRONOQ_OK`: `*out` is the attempt. The host runs it and then calls exactly one of
///   `cronoq_complete`, `cronoq_fail` and `cronoq_release`, and `cronoq_lease_free`.
/// - `CRONOQ_TIMEOUT`: nothing became ready in time. Call again.
/// - `CRONOQ_PAUSED`: nothing became ready in time and the pool is paused; the message is
///   the reason. Call again: the call waits out its timeout, so this is not a busy loop.
/// - `CRONOQ_SHUTDOWN`: the scheduler was shut down. Stop calling.
///
/// Thread safety: may be called from any number of threads at once; each call blocks its
/// thread. Nothing calls back into the host.
///
/// # Safety
/// `s` is a live scheduler. `pool` is null or a zero-terminated string. `out` points to
/// writable memory for one `cronoq_lease`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_next_lease(
    s: *const cronoq_scheduler,
    pool: *const c_char,
    timeout_ms: u64,
    out: *mut cronoq_lease,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller, for every pointer below.
        let s = unsafe { scheduler(s) }?;
        let pool = unsafe { pool_name(pool) }?;
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        if !s.hub.has_pool(pool) {
            return Err(invalid(format!("unknown pool '{pool}'")));
        }
        let timeout = Duration::from_millis(timeout_ms);
        let next = s.with(|b| {
            Ok(s.hub
                .next(pool, timeout, &|| sync_capacity(b, &s.hub, pool)))
        })?;
        let leased = match next {
            Next::Lease(l) => l,
            Next::Closed => {
                return Err(Fail(CRONOQ_SHUTDOWN, "the scheduler was shut down".into()));
            }
            Next::Timeout => {
                let pauses = s.with(|b| Ok(b.block_on(b.scheduler().pauses(pool))?))?;
                return match pauses.pool {
                    Some(reason) => Err(Fail(CRONOQ_PAUSED, reason.to_string())),
                    None => Ok(CRONOQ_TIMEOUT),
                };
            }
        };
        let ctx = &leased.ctx;
        let mut info = json!({
            "pool": &*ctx.pool,
            "tags": ctx.tags.iter().map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<std::collections::BTreeMap<_, _>>(),
        });
        if let Some(d) = ctx.deadline
            && let Ok(since) = d.duration_since(UNIX_EPOCH)
        {
            info["deadline_unix_ms"] = json!(since.as_millis() as u64);
        }
        if let Some(t) = ctx.timeout {
            info["timeout_ms"] = json!(t.as_millis() as u64);
        }
        let owner = Box::new(LeaseOwner {
            _payload: leased.payload.clone(),
            _key: ctx.key.as_deref().map(c_string),
            _kind: c_string(&ctx.kind),
            _info: c_string(&info.to_string()),
        });
        let lease = cronoq_lease {
            lease: ctx.lease.0,
            task: ctx.id.0,
            attempt: ctx.attempt,
            payload: if leased.payload.is_empty() {
                std::ptr::null()
            } else {
                leased.payload.as_ptr()
            },
            payload_len: leased.payload.len(),
            key: owner._key.as_ref().map_or(std::ptr::null(), |k| k.as_ptr()),
            kind: owner._kind.as_ptr(),
            info_json: owner._info.as_ptr(),
            owner: Box::into_raw(owner).cast(),
        };
        unsafe { out.write(lease) };
        Ok(CRONOQ_OK)
    })
}

/// Release what a lease from `cronoq_next_lease` points to, and clear it. This does not end
/// the attempt: call `cronoq_complete`, `cronoq_fail` or `cronoq_release` for that, before
/// or after. Null, and a lease that was already freed, are allowed.
///
/// Thread safety: one thread at a time per lease.
///
/// # Safety
/// `lease` is null or points to a lease that `cronoq_next_lease` filled (or that is zeroed).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_lease_free(lease: *mut cronoq_lease) {
    let _ = guard(|| {
        // SAFETY: caller.
        let Some(l) = (unsafe { lease.as_mut() }) else {
            return Ok(CRONOQ_OK);
        };
        if !l.owner.is_null() {
            // SAFETY: `owner` came from `Box::into_raw` in `cronoq_next_lease` and is
            // cleared below, so it is freed once.
            drop(unsafe { Box::from_raw(l.owner.cast::<LeaseOwner>()) });
        }
        l.owner = std::ptr::null_mut();
        l.payload = std::ptr::null();
        l.payload_len = 0;
        l.key = std::ptr::null();
        l.kind = std::ptr::null();
        l.info_json = std::ptr::null();
        Ok(CRONOQ_OK)
    });
}

/// End a lease with `result`.
fn finish(
    s: &cronoq_scheduler,
    lease: u64,
    result: Result<Bytes, TaskError>,
) -> Res<cronoq_status> {
    s.with(|b| {
        let held = s
            .hub
            .finish(lease, result, &|pool| sync_capacity(b, &s.hub, pool));
        Ok(if held { CRONOQ_OK } else { CRONOQ_LEASE_GONE })
    })
}

/// Report that the attempt succeeded. The task is done.
///
/// Returns `CRONOQ_LEASE_GONE` if the lease is over (timed out, cancelled, already ended):
/// the report is ignored.
///
/// Thread safety: may be called from any thread, not only the one that took the lease.
///
/// # Safety
/// `s` is a live scheduler.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_complete(s: *const cronoq_scheduler, lease: u64) -> cronoq_status {
    // SAFETY: caller.
    guard(|| finish(unsafe { scheduler(s) }?, lease, Ok(Bytes::new())))
}

/// Report that the attempt failed. `message` (null allowed) is kept with the task.
///
/// With `retryable` not 0 the task is tried again after its backoff, at least
/// `retry_after_ms` from now when that is not negative, until its retry policy is used up.
/// With `retryable` 0 the task fails for good.
///
/// Returns `CRONOQ_LEASE_GONE` if the lease is over: the report is ignored.
///
/// Thread safety: may be called from any thread.
///
/// # Safety
/// `s` is a live scheduler. `message` is null or a zero-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_fail(
    s: *const cronoq_scheduler,
    lease: u64,
    message: *const c_char,
    retryable: i32,
    retry_after_ms: i64,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller.
        let s = unsafe { scheduler(s) }?;
        let message = unsafe { text("message", message) }?.unwrap_or("failed");
        let err = if retryable == 0 {
            TaskError::permanent(message)
        } else {
            match u64::try_from(retry_after_ms) {
                Ok(ms) => TaskError::retry_after(message, Duration::from_millis(ms)),
                Err(_) => TaskError::retryable(message),
            }
        };
        finish(s, lease, Err(err))
    })
}

/// Give the attempt back without a verdict: the host cannot run the task now, and nothing is
/// wrong with the task. The task keeps its place and is handed out again after `delay_ms`.
/// This does not use up a retry, up to the pool's or the task's `max_releases`; beyond that
/// it counts as a retryable failure.
///
/// Returns `CRONOQ_LEASE_GONE` if the lease is over: the report is ignored.
///
/// Thread safety: may be called from any thread.
///
/// # Safety
/// `s` is a live scheduler.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_release(
    s: *const cronoq_scheduler,
    lease: u64,
    delay_ms: u64,
) -> cronoq_status {
    guard(|| {
        let err = TaskError::release(Duration::from_millis(delay_ms));
        // SAFETY: caller.
        finish(unsafe { scheduler(s) }?, lease, Err(err))
    })
}

/// Tell the scheduler the host is still working on a lease. Needed only for pools with
/// `lease_ttl_ms`: a lease that is not extended within that time expires, and its task is
/// tried again.
///
/// Returns `CRONOQ_LEASE_GONE` if the lease is over (timed out, its task was cancelled): the
/// host should stop working on it. This is how a long attempt learns of a cancel.
///
/// Thread safety: may be called from any thread.
///
/// # Safety
/// `s` is a live scheduler.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_heartbeat(s: *const cronoq_scheduler, lease: u64) -> cronoq_status {
    guard(|| {
        // SAFETY: caller.
        let s = unsafe { scheduler(s) }?;
        s.with(|_| {
            Ok(if s.hub.heartbeat(lease) {
                CRONOQ_OK
            } else {
                CRONOQ_LEASE_GONE
            })
        })
    })
}

/// Cancel a task: the one with `key` when `key` is not null, else the one with id `task`.
/// A queued task is removed; the lease of a running one ends (`CRONOQ_LEASE_GONE` on its
/// next heartbeat or report).
///
/// `*out_cancelled` (null allowed) is 1 if a live task was cancelled, 0 if there was none.
///
/// Thread safety: may be called from any number of threads at once.
///
/// # Safety
/// `s` is a live scheduler. `key` is null or a zero-terminated string. `out_cancelled` is
/// null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_cancel(
    s: *const cronoq_scheduler,
    task: u64,
    key: *const c_char,
    out_cancelled: *mut i32,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller, for every pointer below.
        let s = unsafe { scheduler(s) }?;
        let target = unsafe { task_ref(task, key) }?;
        let cancelled = s.with(|b| Ok(b.cancel(target)?))?;
        if !out_cancelled.is_null() {
            unsafe { *out_cancelled = cancelled as i32 };
        }
        Ok(CRONOQ_OK)
    })
}

/// Change a queued task (by `key` when not null, else by id `task`) without losing its place
/// or its age. `update_json` is a JSON object:
///
/// ```json
/// {"priority": "high", "deadline_ms": 2000, "no_deadline": false, "tags": {"a": "b"}}
/// ```
///
/// Every field is optional: `priority` sets the class, `deadline_ms` a deadline that far
/// from now, `no_deadline` removes the deadline, `tags` replaces the tags.
///
/// `*out_changed` (null allowed) is 1 if something changed. A task that is running is
/// changed for its next attempt. Returns `CRONOQ_NOT_FOUND` if there is no such live task.
///
/// Thread safety: may be called from any number of threads at once.
///
/// # Safety
/// `s` is a live scheduler. `key` is null or a zero-terminated string. `update_json` is a
/// zero-terminated string. `out_changed` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_update(
    s: *const cronoq_scheduler,
    task: u64,
    key: *const c_char,
    update_json: *const c_char,
    out_changed: *mut i32,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller, for every pointer below.
        let s = unsafe { scheduler(s) }?;
        let target = unsafe { task_ref(task, key) }?;
        let update =
            UpdateJson::parse(unsafe { required("update_json", update_json) }?).map_err(invalid)?;
        let done = s.with(|b| Ok(b.update(target, update)?))?;
        if !out_changed.is_null() {
            unsafe { *out_changed = done.changed as i32 };
        }
        Ok(CRONOQ_OK)
    })
}

/// Set the lowest class the tasks of an ordering key run at, so that one urgent task lifts
/// the whole key. `floor` is 0 critical, 1 high, 2 normal, 3 low, or -1 to remove the floor.
/// With `persist` not 0 the floor stays when the key has no tasks left.
///
/// Thread safety: may be called from any number of threads at once.
///
/// # Safety
/// `s` is a live scheduler. `pool` is null (`default`) or a zero-terminated string.
/// `ordering_key` is a zero-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_set_ordering_floor(
    s: *const cronoq_scheduler,
    pool: *const c_char,
    ordering_key: *const c_char,
    floor: i32,
    persist: i32,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller, for every pointer below.
        let s = unsafe { scheduler(s) }?;
        let pool = unsafe { pool_name(pool) }?;
        let key = unsafe { required("ordering_key", ordering_key) }?;
        let floor = if floor == -1 {
            None
        } else {
            Some(class(floor)?)
        };
        s.with(|b| {
            Ok(b.block_on(
                b.scheduler()
                    .set_ordering_floor(pool, key, floor, persist != 0),
            )?)
        })?;
        Ok(CRONOQ_OK)
    })
}

/// What a pause or a resume names: an ordering key if given, else a class if not -1, else
/// the pool.
///
/// # Safety
/// As for [`text`].
unsafe fn pause_target(class_or_minus_one: i32, ordering_key: *const c_char) -> Res<PauseTarget> {
    // SAFETY: passed on.
    let key = unsafe { text("ordering_key", ordering_key) }?;
    match (key, class_or_minus_one) {
        (Some(_), c) if c != -1 => Err(invalid("name a class or an ordering key, not both")),
        (Some(""), _) => Err(invalid("ordering_key is empty")),
        (Some(k), _) => Ok(PauseTarget::Ordering(k.into())),
        (None, -1) => Ok(PauseTarget::Pool),
        (None, c) => Ok(PauseTarget::Class(class(c)?)),
    }
}

/// Pause a pool, one class of it, or one ordering key of it: its tasks stay queued and keep
/// their place, and none of them is handed out until it is resumed. Attempts that are
/// running are not interrupted.
///
/// `pool` null is `default`. `priority_class` is 0 critical to 3 low, or -1 for none.
/// `ordering_key` is null for none. With neither, the whole pool is paused. `reason` is
/// required and is shown wherever the pause is.
///
/// `*out_changed` (null allowed) is 1 if this paused something that was not paused, 0 if it
/// only replaced the reason.
///
/// Thread safety: may be called from any number of threads at once.
///
/// # Safety
/// `s` is a live scheduler. `pool` and `ordering_key` are null or zero-terminated strings.
/// `reason` is a zero-terminated string. `out_changed` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_pause(
    s: *const cronoq_scheduler,
    pool: *const c_char,
    priority_class: i32,
    ordering_key: *const c_char,
    reason: *const c_char,
    out_changed: *mut i32,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller, for every pointer below.
        let s = unsafe { scheduler(s) }?;
        let pool = unsafe { pool_name(pool) }?;
        let target = unsafe { pause_target(priority_class, ordering_key) }?;
        let reason = unsafe { required("reason", reason) }?;
        if reason.trim().is_empty() {
            return Err(invalid("reason is empty"));
        }
        let changed = s.with(|b| Ok(b.block_on(b.scheduler().pause(pool, target, reason))?))?;
        if !out_changed.is_null() {
            unsafe { *out_changed = changed as i32 };
        }
        Ok(CRONOQ_OK)
    })
}

/// Resume what `cronoq_pause` paused; the arguments name it the same way. Resuming an
/// ordering key also resumes a key that a failed task halted.
///
/// `*out_changed` (null allowed) is 1 if something was resumed, 0 if nothing was paused.
///
/// Thread safety: may be called from any number of threads at once.
///
/// # Safety
/// `s` is a live scheduler. `pool` and `ordering_key` are null or zero-terminated strings.
/// `out_changed` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_resume(
    s: *const cronoq_scheduler,
    pool: *const c_char,
    priority_class: i32,
    ordering_key: *const c_char,
    out_changed: *mut i32,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller, for every pointer below.
        let s = unsafe { scheduler(s) }?;
        let pool = unsafe { pool_name(pool) }?;
        let target = unsafe { pause_target(priority_class, ordering_key) }?;
        let changed = s.with(|b| Ok(b.block_on(b.scheduler().resume(pool, target))?))?;
        if !out_changed.is_null() {
            unsafe { *out_changed = changed as i32 };
        }
        Ok(CRONOQ_OK)
    })
}

/// `CamelCase` as `snake_case`, for the names of statuses.
fn snake(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// Describe a live task (by `key` when not null, else by id `task`) as JSON:
///
/// ```json
/// {"id": 7, "key": "k", "pool": "default", "kind": "", "priority": "normal",
///  "status": "ready", "attempt": 0}
/// ```
///
/// `status` is one of `blocked`, `scheduled`, `ready`, `running`, `retrying` for a live
/// task, or a final one (`succeeded`, `failed`, `dead_lettered`, `cancelled`, ...) for a
/// short time after the task ended. Returns `CRONOQ_NOT_FOUND` if the scheduler does not
/// know the task (any more).
///
/// Ownership: on `CRONOQ_OK` the host owns `*out` and frees it with `cronoq_buf_free`.
///
/// Thread safety: may be called from any number of threads at once.
///
/// # Safety
/// `s` is a live scheduler. `key` is null or a zero-terminated string. `out` points to
/// writable memory for one `cronoq_buf`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_task(
    s: *const cronoq_scheduler,
    task: u64,
    key: *const c_char,
    out: *mut cronoq_buf,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller, for every pointer below.
        let s = unsafe { scheduler(s) }?;
        let target = unsafe { task_ref(task, key) }?;
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        let found = s.with(|b| Ok(b.block_on(b.scheduler().task(target))?))?;
        let Some(t) = found else {
            return Err(Fail(CRONOQ_NOT_FOUND, "no such task".into()));
        };
        let text = json!({
            "id": t.id.0,
            "key": t.key.as_deref(),
            "pool": &*t.pool,
            "kind": &*t.kind,
            "priority": t.priority.name(),
            "status": snake(&format!("{:?}", t.status)),
            "attempt": t.attempt,
        })
        .to_string();
        unsafe { out.write(buf(text)) };
        Ok(CRONOQ_OK)
    })
}

/// The state of the scheduler as JSON: per pool the queue lengths per class, what is paused
/// and why, the ordering keys, and the totals since start.
///
/// ```json
/// {"pools": [{"name": "default", "capacity": 2, "busy": 1, "ready": 5,
///             "pending": {"critical": 0, "high": 1, "normal": 4, "low": 0},
///             "paused": null, "paused_classes": {"low": "backfill off"},
///             "paused_tasks": 0,
///             "ordering": {"keys": 3, "waiting": 2, "halted": 0, "paused": 0}}],
///  "live_tasks": 6, "running": 1, "pending": 5,
///  "totals": {"submitted": 9, "dispatched": 4, "succeeded": 3, "failed": 0,
///             "dead_lettered": 0, "cancelled": 0, "shed": 0, "retries": 0, "timeouts": 0,
///             "lease_expiries": 0, "superseded": 0, "releases": 0}}
/// ```
///
/// Ownership: on `CRONOQ_OK` the host owns `*out` and frees it with `cronoq_buf_free`.
///
/// Thread safety: may be called from any number of threads at once.
///
/// # Safety
/// `s` is a live scheduler. `out` points to writable memory for one `cronoq_buf`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_snapshot(
    s: *const cronoq_scheduler,
    out: *mut cronoq_buf,
) -> cronoq_status {
    guard(|| {
        // SAFETY: caller.
        let s = unsafe { scheduler(s) }?;
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        let snap = s.with(|b| Ok(b.snapshot()?))?;
        let names = ["critical", "high", "normal", "low"];
        let pools: Vec<_> = snap
            .pools
            .iter()
            .map(|p| {
                let pending: serde_json::Map<_, _> = names
                    .iter()
                    .zip(p.pending_by_class)
                    .map(|(n, v)| (n.to_string(), json!(v)))
                    .collect();
                let classes: serde_json::Map<_, _> = names
                    .iter()
                    .zip(&p.pauses.classes)
                    .filter_map(|(n, r)| Some((n.to_string(), json!(&**r.as_ref()?))))
                    .collect();
                json!({
                    "name": &*p.name,
                    "capacity": p.capacity,
                    "busy": p.busy,
                    "ready": p.ready,
                    "pending": pending,
                    "paused": p.pauses.pool.as_deref(),
                    "paused_classes": classes,
                    "paused_tasks": p.paused_tasks,
                    "ordering": {
                        "keys": p.ordering.keys,
                        "waiting": p.ordering.waiting,
                        "halted": p.ordering.halted,
                        "paused": p.ordering.paused,
                    },
                })
            })
            .collect();
        let t = &snap.totals;
        let text = json!({
            "pools": pools,
            "live_tasks": snap.live_tasks,
            "running": snap.running,
            "pending": snap.pending,
            "totals": {
                "submitted": t.submitted,
                "dispatched": t.dispatched,
                "succeeded": t.succeeded,
                "failed": t.failed,
                "dead_lettered": t.dead_lettered,
                "cancelled": t.cancelled,
                "shed": t.shed,
                "retries": t.retries,
                "timeouts": t.timeouts,
                "lease_expiries": t.lease_expiries,
                "superseded": t.superseded,
                "releases": t.releases,
            },
        })
        .to_string();
        // SAFETY: caller.
        unsafe { out.write(buf(text)) };
        Ok(CRONOQ_OK)
    })
}

/// Free bytes the library returned (`cronoq_task`, `cronoq_snapshot`) and clear the buffer.
/// Null, and a buffer that was already freed, are allowed.
///
/// Thread safety: one thread at a time per buffer.
///
/// # Safety
/// `b` is null or points to a buffer the library filled (or that is zeroed).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cronoq_buf_free(b: *mut cronoq_buf) {
    let _ = guard(|| {
        // SAFETY: caller.
        let Some(b) = (unsafe { b.as_mut() }) else {
            return Ok(CRONOQ_OK);
        };
        if !b.ptr.is_null() {
            // SAFETY: `ptr` and `len + 1` are what `buf` leaked as a boxed slice.
            drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(b.ptr, b.len + 1)) });
        }
        b.ptr = std::ptr::null_mut();
        b.len = 0;
        Ok(CRONOQ_OK)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_becomes_a_status_and_a_message() {
        let status = guard(|| panic!("boom {}", 7));
        assert_eq!(status, CRONOQ_INTERNAL);
        let msg = unsafe { CStr::from_ptr(cronoq_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(msg, "panic inside cronoq: boom 7");
    }

    #[test]
    fn status_names_are_snake_case() {
        assert_eq!(snake("DeadLettered"), "dead_lettered");
        assert_eq!(snake("Ready"), "ready");
    }
}
