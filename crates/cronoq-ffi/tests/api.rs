//! The C API, called the way a host calls it.

use std::ffi::{CStr, CString, c_char};
use std::ptr::{null, null_mut};

use cronoq_ffi::cronoq_status::*;
use cronoq_ffi::*;

/// A scheduler that is shut down and freed when the test ends.
struct Sched(*mut cronoq_scheduler);

// The API is made for use from several threads.
unsafe impl Send for Sched {}
unsafe impl Sync for Sched {}

impl Drop for Sched {
    fn drop(&mut self) {
        unsafe {
            cronoq_shutdown(self.0, 0);
            cronoq_free(self.0);
        }
    }
}

fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

fn last_error() -> String {
    unsafe { CStr::from_ptr(cronoq_last_error()) }
        .to_str()
        .unwrap()
        .to_string()
}

fn new(config: &str) -> Sched {
    let mut s = null_mut();
    let status = unsafe { cronoq_new(c(config).as_ptr(), &mut s) };
    assert_eq!(status, CRONOQ_OK, "{}", last_error());
    Sched(s)
}

fn submit_full(s: &Sched, payload: &str, options: &str) -> (cronoq_status, u64, i32) {
    let (mut id, mut outcome) = (0, -1);
    let status = unsafe {
        cronoq_submit(
            s.0,
            payload.as_ptr(),
            payload.len(),
            c(options).as_ptr(),
            &mut id,
            &mut outcome,
        )
    };
    (status, id, outcome)
}

fn submit(s: &Sched, payload: &str, options: &str) -> u64 {
    let (status, id, _) = submit_full(s, payload, options);
    assert_eq!(status, CRONOQ_OK, "{}", last_error());
    id
}

/// A lease that was taken, copied out of the C structure.
#[derive(Debug)]
struct Taken {
    lease: u64,
    task: u64,
    attempt: u32,
    payload: String,
    key: Option<String>,
    info: serde_json::Value,
}

fn text(p: *const c_char) -> String {
    unsafe { CStr::from_ptr(p) }.to_str().unwrap().to_string()
}

fn next_in(s: &Sched, pool: Option<&str>, timeout_ms: u64) -> Result<Taken, cronoq_status> {
    let mut l = std::mem::MaybeUninit::<cronoq_lease>::zeroed();
    let pool = pool.map(c);
    let status = unsafe {
        cronoq_next_lease(
            s.0,
            pool.as_ref().map_or(null(), |p| p.as_ptr()),
            timeout_ms,
            l.as_mut_ptr(),
        )
    };
    if status != CRONOQ_OK {
        return Err(status);
    }
    let mut l = unsafe { l.assume_init() };
    let payload = if l.payload.is_null() {
        String::new()
    } else {
        let bytes = unsafe { std::slice::from_raw_parts(l.payload, l.payload_len) };
        String::from_utf8(bytes.to_vec()).unwrap()
    };
    let taken = Taken {
        lease: l.lease,
        task: l.task,
        attempt: l.attempt,
        payload,
        key: (!l.key.is_null()).then(|| text(l.key)),
        info: serde_json::from_str(&text(l.info_json)).unwrap(),
    };
    unsafe {
        cronoq_lease_free(&mut l);
        // Freeing twice is allowed.
        cronoq_lease_free(&mut l);
    }
    assert!(l.payload.is_null() && l.owner.is_null());
    Ok(taken)
}

fn next(s: &Sched) -> Taken {
    next_in(s, None, 5000).unwrap_or_else(|e| panic!("no lease: {e:?} {}", last_error()))
}

fn complete(s: &Sched, t: &Taken) {
    assert_eq!(
        unsafe { cronoq_complete(s.0, t.lease, null(), 0) },
        CRONOQ_OK
    );
}

/// Take and complete leases until the queue is empty; their payloads in order.
fn drain(s: &Sched) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(t) = next_in(s, None, 100) {
        complete(s, &t);
        out.push(t.payload);
    }
    out
}

fn json(b: &mut cronoq_buf) -> serde_json::Value {
    let v = serde_json::from_str(&text(b.ptr.cast())).unwrap();
    unsafe {
        cronoq_buf_free(b);
        cronoq_buf_free(b);
    }
    assert!(b.ptr.is_null());
    v
}

fn snapshot(s: &Sched) -> serde_json::Value {
    let mut b = cronoq_buf {
        ptr: null_mut(),
        len: 0,
    };
    assert_eq!(unsafe { cronoq_snapshot(s.0, &mut b) }, CRONOQ_OK);
    json(&mut b)
}

/// Reports are applied by the scheduler's thread shortly after the call returns: wait for
/// the snapshot to show them.
fn eventually(s: &Sched, what: &str, ok: impl Fn(&serde_json::Value) -> bool) -> serde_json::Value {
    let start = std::time::Instant::now();
    loop {
        let snap = snapshot(s);
        if ok(&snap) {
            return snap;
        }
        assert!(start.elapsed().as_secs() < 5, "{what}: {snap}");
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

fn task(s: &Sched, id: u64) -> Option<serde_json::Value> {
    let mut b = cronoq_buf {
        ptr: null_mut(),
        len: 0,
    };
    match unsafe { cronoq_task(s.0, id, null(), &mut b) } {
        CRONOQ_OK => Some(json(&mut b)),
        CRONOQ_NOT_FOUND => None,
        other => panic!("{other:?}: {}", last_error()),
    }
}

#[test]
fn the_version_is_reported() {
    assert_eq!(cronoq_abi_version(), CRONOQ_ABI_VERSION);
    assert_eq!(text(cronoq_version()), env!("CARGO_PKG_VERSION"));
}

#[test]
fn tasks_are_handed_out_in_priority_order_chosen_when_asked() {
    let s = new("");
    // Nobody waits for a lease yet, so nothing is started: the order is still open.
    submit(&s, "low", r#"{"priority": "low"}"#);
    submit(&s, "normal", "");
    submit(&s, "critical", r#"{"priority": "critical"}"#);
    submit(&s, "high", r#"{"priority": "high"}"#);
    assert_eq!(snapshot(&s)["running"], 0);
    assert_eq!(drain(&s), ["critical", "high", "normal", "low"]);
    let snap = eventually(&s, "four done", |v| v["totals"]["succeeded"] == 4);
    assert_eq!(snap["pools"][0]["name"], "default");
    assert_eq!(snap["pools"][0]["busy"], 0);
}

#[test]
fn a_lease_carries_the_task() {
    let s = new("");
    let id = submit(
        &s,
        "hello",
        r#"{"key": "k1", "kind": "send", "tags": {"a": "b"}, "timeout_ms": 60000}"#,
    );
    let t = next(&s);
    assert_eq!((t.task, t.attempt), (id, 1));
    assert_eq!(t.payload, "hello");
    assert_eq!(t.key.as_deref(), Some("k1"));
    assert_eq!(t.info["pool"], "default");
    assert_eq!(t.info["tags"]["a"], "b");
    assert_eq!(t.info["timeout_ms"], 60000);
    assert_eq!(task(&s, id).unwrap()["status"], "running");
    complete(&s, &t);
    // A second report for the same lease is ignored.
    assert_eq!(
        unsafe { cronoq_complete(s.0, t.lease, null(), 0) },
        CRONOQ_LEASE_GONE
    );

    // An empty payload is a null pointer with length 0.
    assert_eq!(
        unsafe { cronoq_submit(s.0, null(), 0, null(), null_mut(), null_mut()) },
        CRONOQ_OK
    );
    let t = next(&s);
    assert_eq!(t.payload, "");
    assert_eq!(t.key, None);
    complete(&s, &t);
}

#[test]
fn an_ordering_key_keeps_its_tasks_in_order_and_a_floor_lifts_them() {
    let s = new("");
    submit(&s, "a1", r#"{"ordering_key": "a", "priority": "low"}"#);
    submit(&s, "a2", r#"{"ordering_key": "a", "priority": "low"}"#);
    submit(&s, "n", "");
    // One task of a key at a time: the second waits for the first.
    let key = c("a");
    assert_eq!(
        unsafe { cronoq_set_ordering_floor(s.0, null(), key.as_ptr(), 1, 0) },
        CRONOQ_OK,
        "{}",
        last_error()
    );
    assert_eq!(snapshot(&s)["pools"][0]["ordering"]["keys"], 1);
    // Lifted to high, the key's tasks go before the normal one, in their own order.
    assert_eq!(drain(&s), ["a1", "a2", "n"]);
    assert_eq!(
        unsafe { cronoq_set_ordering_floor(s.0, null(), key.as_ptr(), 9, 0) },
        CRONOQ_INVALID_ARGUMENT
    );
}

#[test]
fn a_submission_can_replace_the_queued_task_with_its_key() {
    let s = new("");
    let first = submit(&s, "v1", r#"{"key": "title"}"#);
    let (status, id, outcome) = submit_full(
        &s,
        "v2",
        r#"{"key": "title", "on_key_conflict": "replace_queued"}"#,
    );
    assert_eq!((status, id, outcome), (CRONOQ_OK, first, 2));
    // Without an instruction the live task is returned.
    let (status, id, outcome) = submit_full(&s, "v3", r#"{"key": "title"}"#);
    assert_eq!((status, id, outcome), (CRONOQ_OK, first, 1));
    // A different fingerprint for a taken key conflicts.
    let other = submit(&s, "f1", r#"{"key": "f", "fingerprint": "0a"}"#);
    let (status, ..) = submit_full(
        &s,
        "f2",
        r#"{"key": "f", "fingerprint": "0b", "on_key_conflict": "reject"}"#,
    );
    assert_eq!(status, CRONOQ_CONFLICT, "{}", last_error());
    assert!(task(&s, other).is_some());
    assert_eq!(drain(&s), ["v2", "f1"]);
}

#[test]
fn a_paused_pool_hands_out_nothing_and_says_why() {
    let s = new("");
    submit(&s, "x", "");
    let reason = c("relay deploy");
    let mut changed = -1;
    assert_eq!(
        unsafe { cronoq_pause(s.0, null(), -1, null(), reason.as_ptr(), &mut changed) },
        CRONOQ_OK
    );
    assert_eq!(changed, 1);
    assert_eq!(next_in(&s, None, 50).unwrap_err(), CRONOQ_PAUSED);
    assert_eq!(last_error(), "relay deploy");
    let snap = snapshot(&s);
    assert_eq!(snap["pools"][0]["paused"], "relay deploy");
    assert_eq!(snap["pools"][0]["paused_tasks"], 1);
    assert_eq!(
        unsafe { cronoq_resume(s.0, null(), -1, null(), &mut changed) },
        CRONOQ_OK
    );
    assert_eq!(changed, 1);
    assert_eq!(drain(&s), ["x"]);

    // A class, and an ordering key.
    submit(&s, "low", r#"{"priority": "low"}"#);
    submit(&s, "keyed", r#"{"ordering_key": "k"}"#);
    submit(&s, "free", "");
    let key = c("k");
    unsafe {
        cronoq_pause(s.0, null(), 3, null(), reason.as_ptr(), null_mut());
        cronoq_pause(s.0, null(), -1, key.as_ptr(), reason.as_ptr(), null_mut());
    }
    assert_eq!(drain(&s), ["free"]);
    assert_eq!(
        snapshot(&s)["pools"][0]["paused_classes"]["low"],
        "relay deploy"
    );
    // Only the pool as a whole is reported as paused by `cronoq_next_lease`.
    assert_eq!(next_in(&s, None, 20).unwrap_err(), CRONOQ_TIMEOUT);
    unsafe {
        cronoq_resume(s.0, null(), 3, null(), null_mut());
        cronoq_resume(s.0, null(), -1, key.as_ptr(), null_mut());
    }
    assert_eq!(drain(&s), ["keyed", "low"]);

    // A reason is required, and a class and a key exclude each other.
    assert_eq!(
        unsafe { cronoq_pause(s.0, null(), -1, null(), null(), null_mut()) },
        CRONOQ_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { cronoq_pause(s.0, null(), 1, key.as_ptr(), reason.as_ptr(), null_mut()) },
        CRONOQ_INVALID_ARGUMENT
    );
}

#[test]
fn a_released_attempt_comes_back_without_using_a_retry() {
    let s = new(r#"{"pools": [{"name": "default", "retry": {"max_attempts": 1}}]}"#);
    let id = submit(&s, "x", "");
    let first = next(&s);
    assert_eq!(unsafe { cronoq_release(s.0, first.lease, 0) }, CRONOQ_OK);
    let second = next(&s);
    assert_eq!((second.task, second.attempt), (id, 2));
    complete(&s, &second);
    let snap = eventually(&s, "done", |v| v["totals"]["succeeded"] == 1);
    assert_eq!(snap["totals"]["releases"], 1);
    assert_eq!(snap["totals"]["retries"], 0);
}

#[test]
fn a_failed_attempt_is_retried_or_ends_the_task() {
    let s = new(r#"{"pools": [{"name": "default",
            "retry": {"max_attempts": 2, "initial_backoff_ms": 1, "jitter": false}}]}"#);
    let id = submit(&s, "x", "");
    let msg = c("relay said no");
    let t = next(&s);
    assert_eq!(
        unsafe { cronoq_fail(s.0, t.lease, msg.as_ptr(), 1, -1) },
        CRONOQ_OK
    );
    let t = next(&s);
    assert_eq!((t.task, t.attempt), (id, 2));
    // The policy is used up: the task ends.
    assert_eq!(
        unsafe { cronoq_fail(s.0, t.lease, msg.as_ptr(), 1, 5) },
        CRONOQ_OK
    );
    assert_eq!(next_in(&s, None, 50).unwrap_err(), CRONOQ_TIMEOUT);
    eventually(&s, "dead", |v| v["totals"]["dead_lettered"] == 1);

    // Not retryable: it fails for good at once.
    submit(&s, "y", "");
    let t = next(&s);
    assert_eq!(
        unsafe { cronoq_fail(s.0, t.lease, null(), 0, -1) },
        CRONOQ_OK
    );
    assert_eq!(next_in(&s, None, 50).unwrap_err(), CRONOQ_TIMEOUT);
    eventually(&s, "failed", |v| v["totals"]["failed"] == 1);
}

#[test]
fn a_queued_task_can_be_updated_and_cancelled() {
    let s = new("");
    submit(&s, "normal", "");
    let low = submit(&s, "was-low", r#"{"priority": "low", "key": "k"}"#);
    let gone = submit(&s, "cancelled", "");
    let up = c(r#"{"priority": "critical"}"#);
    let key = c("k");
    let mut changed = -1;
    assert_eq!(
        unsafe { cronoq_update(s.0, 0, key.as_ptr(), up.as_ptr(), &mut changed) },
        CRONOQ_OK,
        "{}",
        last_error()
    );
    assert_eq!(changed, 1);
    assert_eq!(task(&s, low).unwrap()["priority"], "critical");
    let mut cancelled = -1;
    assert_eq!(
        unsafe { cronoq_cancel(s.0, gone, null(), &mut cancelled) },
        CRONOQ_OK
    );
    assert_eq!(cancelled, 1);
    assert_eq!(
        unsafe { cronoq_cancel(s.0, gone, null(), &mut cancelled) },
        CRONOQ_OK
    );
    assert_eq!(cancelled, 0);
    assert_eq!(drain(&s), ["was-low", "normal"]);
    assert_eq!(
        unsafe { cronoq_update(s.0, 999_999, null(), up.as_ptr(), null_mut()) },
        CRONOQ_NOT_FOUND
    );
    let bad = c(r#"{"priorty": "high"}"#);
    assert_eq!(
        unsafe { cronoq_update(s.0, low, null(), bad.as_ptr(), null_mut()) },
        CRONOQ_INVALID_ARGUMENT
    );
    assert!(last_error().contains("priorty"), "{}", last_error());
}

#[test]
fn cancelling_a_running_task_ends_its_lease() {
    let s = new("");
    let id = submit(&s, "x", "");
    let t = next(&s);
    assert_eq!(unsafe { cronoq_heartbeat(s.0, t.lease) }, CRONOQ_OK);
    assert_eq!(
        unsafe { cronoq_cancel(s.0, id, null(), null_mut()) },
        CRONOQ_OK
    );
    assert_eq!(unsafe { cronoq_heartbeat(s.0, t.lease) }, CRONOQ_LEASE_GONE);
    assert_eq!(
        unsafe { cronoq_complete(s.0, t.lease, null(), 0) },
        CRONOQ_LEASE_GONE
    );
    // The next task may or may not have been started for the slot of the ended lease; either
    // way it goes to the next thread that asks, without a release or a retry.
    submit(&s, "y", "");
    let t = next(&s);
    assert_eq!((t.payload.as_str(), t.attempt), ("y", 1));
    complete(&s, &t);
    let snap = eventually(&s, "idle", |v| {
        v["running"] == 0 && v["pools"][0]["capacity"] == 0 && v["totals"]["succeeded"] == 1
    });
    assert_eq!(snap["totals"]["releases"], 0);
}

#[test]
fn an_attempt_that_times_out_is_taken_back() {
    let s = new(r#"{"pools": [{"name": "default",
            "retry": {"max_attempts": 2, "initial_backoff_ms": 1, "jitter": false}}]}"#);
    let id = submit(&s, "slow", r#"{"timeout_ms": 30}"#);
    let first = next(&s);
    let second = next(&s);
    assert_eq!((second.task, second.attempt), (id, 2));
    assert_eq!(
        unsafe { cronoq_complete(s.0, first.lease, null(), 0) },
        CRONOQ_LEASE_GONE
    );
    complete(&s, &second);
    eventually(&s, "timeout", |v| v["totals"]["timeouts"] == 1);
}

#[test]
fn resources_pace_the_work() {
    let s = new(r#"{"resources": [{"kind": "semaphore", "name": "uplink", "permits": 1}]}"#);
    submit(&s, "a", r#"{"resources": {"uplink": 1}}"#);
    submit(&s, "b", r#"{"resources": {"uplink": 1}}"#);
    let a = next(&s);
    // The second task needs the permit the first one holds.
    assert_eq!(next_in(&s, None, 50).unwrap_err(), CRONOQ_TIMEOUT);
    complete(&s, &a);
    assert_eq!(drain(&s), ["b"]);
    let (status, ..) = submit_full(&s, "c", r#"{"resources": {"nope": 1}}"#);
    assert_eq!(status, CRONOQ_INVALID_ARGUMENT);
    assert!(last_error().contains("nope"), "{}", last_error());
}

#[test]
fn several_pools_and_several_threads() {
    let s = std::sync::Arc::new(new(
        r#"{"pools": [{"name": "send"}, {"name": "upload", "max_leases": 2}]}"#,
    ));
    for i in 0..200 {
        let pool = if i % 2 == 0 { "send" } else { "upload" };
        submit(&s, &i.to_string(), &format!(r#"{{"pool": "{pool}"}}"#));
    }
    let mut threads = Vec::new();
    for n in 0..6 {
        let s = s.clone();
        threads.push(std::thread::spawn(move || {
            let pool = if n % 2 == 0 { "send" } else { "upload" };
            let mut done = Vec::new();
            while let Ok(t) = next_in(&s, Some(pool), 200) {
                assert_eq!(t.info["pool"], pool);
                complete(&s, &t);
                done.push(t.payload.parse::<u32>().unwrap());
            }
            done
        }));
    }
    let mut all: Vec<u32> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    all.sort_unstable();
    assert_eq!(all, (0..200).collect::<Vec<_>>());
    let snap = eventually(&s, "all done", |v| v["totals"]["succeeded"] == 200);
    assert_eq!(snap["totals"]["dispatched"], 200);
    assert_eq!(
        next_in(&s, Some("nope"), 0).unwrap_err(),
        CRONOQ_INVALID_ARGUMENT
    );
}

#[test]
fn a_full_pool_refuses_without_blocking() {
    let s = new(r#"{"pools": [{"name": "default", "max_pending": 2}]}"#);
    submit(&s, "a", "");
    submit(&s, "b", "");
    let (status, ..) = submit_full(&s, "c", "");
    assert_eq!(status, CRONOQ_FULL, "{}", last_error());
    assert_eq!(drain(&s), ["a", "b"]);
}

#[test]
fn shutdown_wakes_waiting_threads_and_ends_every_call() {
    let s = std::sync::Arc::new(new(""));
    let waiter = {
        let s = s.clone();
        std::thread::spawn(move || next_in(&s, None, 60_000).unwrap_err())
    };
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert_eq!(unsafe { cronoq_shutdown(s.0, 0) }, CRONOQ_OK);
    assert_eq!(waiter.join().unwrap(), CRONOQ_SHUTDOWN);
    let (status, ..) = submit_full(&s, "x", "");
    assert_eq!(status, CRONOQ_SHUTDOWN);
    assert_eq!(next_in(&s, None, 0).unwrap_err(), CRONOQ_SHUTDOWN);
    assert_eq!(
        unsafe { cronoq_complete(s.0, 1, null(), 0) },
        CRONOQ_SHUTDOWN
    );
    // Twice is allowed.
    assert_eq!(unsafe { cronoq_shutdown(s.0, 0) }, CRONOQ_OK);
}

#[test]
fn wrong_arguments_are_errors_not_crashes() {
    let mut out = null_mut();
    for bad in [
        "{",
        r#"{"pool": []}"#,
        r#"{"pools": [{"name": "a", "max_leases": 0}]}"#,
        r#"{"pools": [{"name": "a"}, {"name": "a"}]}"#,
        r#"{"pools": [{"name": "a", "overflow": "wait"}]}"#,
        r#"{"resources": [{"kind": "mutex", "name": "m"}]}"#,
    ] {
        let status = unsafe { cronoq_new(c(bad).as_ptr(), &mut out) };
        assert_eq!(status, CRONOQ_INVALID_ARGUMENT, "{bad}");
        assert!(out.is_null());
        assert!(!last_error().is_empty());
    }
    assert_eq!(
        unsafe { cronoq_new(null(), null_mut()) },
        CRONOQ_INVALID_ARGUMENT
    );
    #[cfg(not(feature = "sqlite"))]
    {
        let cfg = c(r#"{"store": {"sqlite": "/tmp/x.db"}}"#);
        assert_eq!(
            unsafe { cronoq_new(cfg.as_ptr(), &mut out) },
            CRONOQ_INVALID_ARGUMENT
        );
        assert!(last_error().contains("sqlite"), "{}", last_error());
    }

    let s = new("");
    for bad in [
        r#"{"priority": "urgent"}"#,
        r#"{"pool": "nope"}"#,
        r#"{"fingerprint": "xyz"}"#,
        r#"{"unknown": 1}"#,
        "[]",
    ] {
        let (status, ..) = submit_full(&s, "x", bad);
        assert_eq!(status, CRONOQ_INVALID_ARGUMENT, "{bad}");
    }
    let not_utf8 = [0xffu8, 0];
    let status = unsafe {
        cronoq_submit(
            s.0,
            null(),
            0,
            not_utf8.as_ptr().cast(),
            null_mut(),
            null_mut(),
        )
    };
    assert_eq!(status, CRONOQ_INVALID_ARGUMENT);
    assert_eq!(
        unsafe { cronoq_submit(s.0, null(), 3, null(), null_mut(), null_mut()) },
        CRONOQ_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { cronoq_submit(null(), null(), 0, null(), null_mut(), null_mut()) },
        CRONOQ_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { cronoq_next_lease(s.0, null(), 0, null_mut()) },
        CRONOQ_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { cronoq_complete(s.0, 12345, null(), 0) },
        CRONOQ_LEASE_GONE
    );
    assert_eq!(
        unsafe { cronoq_snapshot(s.0, null_mut()) },
        CRONOQ_INVALID_ARGUMENT
    );
    assert!(task(&s, 424242).is_none());
    unsafe {
        cronoq_free(null_mut());
        cronoq_buf_free(null_mut());
        cronoq_lease_free(null_mut());
    }
}

/// Wait for a task to end: whether it succeeded, and its output or its message.
fn wait(s: &Sched, id: u64, timeout_ms: u64) -> Result<(bool, String), cronoq_status> {
    let mut b = cronoq_buf {
        ptr: null_mut(),
        len: 0,
    };
    let mut ok = -1;
    match unsafe { cronoq_wait(s.0, id, null(), timeout_ms, &mut ok, &mut b) } {
        CRONOQ_OK => {
            let bytes = if b.ptr.is_null() {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(b.ptr, b.len) }.to_vec()
            };
            unsafe { cronoq_buf_free(&mut b) };
            Ok((ok == 1, String::from_utf8(bytes).unwrap()))
        }
        other => {
            unsafe { cronoq_buf_free(&mut b) };
            Err(other)
        }
    }
}

#[test]
fn a_task_has_an_output_that_waiters_get() {
    let s = std::sync::Arc::new(new(
        r#"{"pools": [{"name": "default", "retry": {"max_attempts": 1}}]}"#,
    ));
    let id = submit(&s, "2+2", r#"{"key": "sum"}"#);
    // Not ended yet.
    assert_eq!(wait(&s, id, 20).unwrap_err(), CRONOQ_TIMEOUT);
    // A thread that waits is woken by the result.
    let waiter = {
        let s = s.clone();
        std::thread::spawn(move || wait(&s, id, 5000))
    };
    let t = next(&s);
    let out = b"4\0binary";
    assert_eq!(
        unsafe { cronoq_complete(s.0, t.lease, out.as_ptr(), out.len()) },
        CRONOQ_OK
    );
    assert_eq!(waiter.join().unwrap(), Ok((true, "4\0binary".to_string())));
    // Later callers get it too, also by key.
    assert_eq!(wait(&s, id, 0), Ok((true, "4\0binary".to_string())));
    let mut b = cronoq_buf {
        ptr: null_mut(),
        len: 0,
    };
    let mut ok = -1;
    let key = c("sum");
    assert_eq!(
        unsafe { cronoq_wait(s.0, 0, key.as_ptr(), 0, &mut ok, &mut b) },
        CRONOQ_OK
    );
    assert_eq!((ok, b.len), (1, 8));
    unsafe { cronoq_buf_free(&mut b) };

    // No output is an empty one.
    let empty = submit(&s, "x", "");
    complete(&s, &next(&s));
    assert_eq!(wait(&s, empty, 5000), Ok((true, String::new())));

    // A task that did not succeed: the status and the message.
    let bad = submit(&s, "y", "");
    let msg = c("relay refused");
    let t = next(&s);
    assert_eq!(
        unsafe { cronoq_fail(s.0, t.lease, msg.as_ptr(), 1, -1) },
        CRONOQ_OK
    );
    assert_eq!(
        wait(&s, bad, 5000),
        Ok((false, "dead_lettered: relay refused".to_string()))
    );
    let gone = submit(&s, "z", "");
    unsafe { cronoq_cancel(s.0, gone, null(), null_mut()) };
    let (ok, text) = wait(&s, gone, 5000).unwrap();
    assert!(!ok && text.starts_with("cancelled"), "{text}");

    assert_eq!(wait(&s, 999_999, 0).unwrap_err(), CRONOQ_NOT_FOUND);
    assert_eq!(
        unsafe { cronoq_complete(s.0, 1, null(), 3) },
        CRONOQ_INVALID_ARGUMENT
    );
    // A waiter does not keep a shutdown waiting, and learns of it.
    let live = submit(&s, "w", "");
    let waiter = {
        let s = s.clone();
        std::thread::spawn(move || wait(&s, live, 60_000))
    };
    std::thread::sleep(std::time::Duration::from_millis(50));
    let start = std::time::Instant::now();
    assert_eq!(unsafe { cronoq_shutdown(s.0, 0) }, CRONOQ_OK);
    assert_eq!(waiter.join().unwrap().unwrap_err(), CRONOQ_SHUTDOWN);
    assert!(start.elapsed().as_secs() < 5);
}

/// An open subscription, closed when dropped.
struct Events(*mut cronoq_events);
unsafe impl Send for Events {}

impl Events {
    fn open(s: &Sched) -> Events {
        let mut e = null_mut();
        assert_eq!(unsafe { cronoq_events_open(s.0, &mut e) }, CRONOQ_OK);
        Events(e)
    }
    fn next(&self, timeout_ms: u64) -> Result<serde_json::Value, cronoq_status> {
        let mut b = cronoq_buf {
            ptr: null_mut(),
            len: 0,
        };
        match unsafe { cronoq_events_next(self.0, timeout_ms, &mut b) } {
            CRONOQ_OK => Ok(json(&mut b)),
            other => {
                unsafe { cronoq_buf_free(&mut b) };
                Err(other)
            }
        }
    }
}

impl Drop for Events {
    fn drop(&mut self) {
        unsafe { cronoq_events_close(self.0) };
    }
}

#[test]
fn a_subscription_gets_an_event_when_a_task_ends() {
    let s = new(r#"{"pools": [{"name": "default", "retry": {"max_attempts": 1}}]}"#);
    let first = Events::open(&s);
    let second = Events::open(&s);
    assert_eq!(first.next(20).unwrap_err(), CRONOQ_TIMEOUT);
    let id = submit(&s, "x", r#"{"key": "k", "priority": "high"}"#);
    complete(&s, &next(&s));
    // By default only the end of a task is an event; every subscription gets it.
    for sub in [&first, &second] {
        let e = sub.next(5000).unwrap();
        assert_eq!(e["task"], id);
        assert_eq!(e["key"], "k");
        assert_eq!(e["pool"], "default");
        assert_eq!(e["status"], "succeeded");
        assert_eq!(e["attempt"], 1);
        assert_eq!(e["priority"], "high");
        assert_eq!(e["event"], "transition");
        assert!(e["at_unix_ms"].as_u64().unwrap() > 1_600_000_000_000);
    }
    let bad = submit(&s, "y", "");
    let msg = c("no");
    let t = next(&s);
    unsafe { cronoq_fail(s.0, t.lease, msg.as_ptr(), 0, -1) };
    let e = first.next(5000).unwrap();
    assert_eq!(
        (&e["task"], &e["status"], &e["message"]),
        (
            &serde_json::json!(bad),
            &serde_json::json!("failed"),
            &serde_json::json!("no")
        )
    );
    assert_eq!(first.next(20).unwrap_err(), CRONOQ_TIMEOUT);

    // A waiting reader is woken by a shutdown, and a subscription outlives its scheduler.
    let reader = std::thread::spawn(move || first.next(60_000).unwrap_err());
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert_eq!(unsafe { cronoq_shutdown(s.0, 0) }, CRONOQ_OK);
    assert_eq!(reader.join().unwrap(), CRONOQ_SHUTDOWN);
    drop(s);
    // The event of the failed task is still in the second subscription; then it is over.
    assert_eq!(second.next(100).unwrap()["status"], "failed");
    assert_eq!(second.next(100).unwrap_err(), CRONOQ_SHUTDOWN);
    unsafe { cronoq_events_close(null_mut()) };
}

#[test]
fn with_all_events_every_change_of_status_is_sent() {
    let s = new(r#"{"events": "all"}"#);
    let sub = Events::open(&s);
    let id = submit(&s, "x", r#"{"key": "k"}"#);
    let up = c(r#"{"priority": "high"}"#);
    assert_eq!(
        unsafe { cronoq_update(s.0, id, null(), up.as_ptr(), null_mut()) },
        CRONOQ_OK
    );
    complete(&s, &next(&s));
    let mut seen = Vec::new();
    while let Ok(e) = sub.next(200) {
        assert_eq!(e["task"], id);
        seen.push(format!(
            "{}:{}",
            e["event"].as_str().unwrap(),
            e["status"].as_str().unwrap()
        ));
    }
    assert!(seen.contains(&"updated:ready".to_string()), "{seen:?}");
    assert!(seen.contains(&"transition:running".to_string()), "{seen:?}");
    assert_eq!(seen.last().unwrap(), "transition:succeeded", "{seen:?}");
    let bad = c(r#"{"events": "some"}"#);
    let mut out = null_mut();
    assert_eq!(
        unsafe { cronoq_new(bad.as_ptr(), &mut out) },
        CRONOQ_INVALID_ARGUMENT
    );
}

#[test]
fn a_pool_hands_out_at_most_max_leases_at_a_time() {
    let s = std::sync::Arc::new(new(r#"{"pools": [{"name": "default", "max_leases": 2}]}"#));
    for i in 0..60 {
        submit(&s, &i.to_string(), "");
    }
    let out = std::sync::Arc::new(std::sync::atomic::AtomicI32::new(0));
    let most = std::sync::Arc::new(std::sync::atomic::AtomicI32::new(0));
    let threads: Vec<_> = (0..5)
        .map(|_| {
            let (s, out, most) = (s.clone(), out.clone(), most.clone());
            std::thread::spawn(move || {
                use std::sync::atomic::Ordering::SeqCst;
                let mut done = 0;
                while let Ok(t) = next_in(&s, None, 200) {
                    most.fetch_max(out.fetch_add(1, SeqCst) + 1, SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(2));
                    out.fetch_sub(1, SeqCst);
                    complete(&s, &t);
                    done += 1;
                }
                done
            })
        })
        .collect();
    let done: i32 = threads.into_iter().map(|t| t.join().unwrap()).sum();
    assert_eq!(done, 60);
    assert_eq!(most.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn a_waiting_thread_still_gets_work_when_another_thread_took_its_task() {
    let s = std::sync::Arc::new(new(""));
    let waiter = {
        let s = s.clone();
        std::thread::spawn(move || next_in(&s, None, 5000).map(|t| t.payload))
    };
    std::thread::sleep(std::time::Duration::from_millis(50));
    // "x" is started for the waiting thread; this thread may take it first.
    submit(&s, "x", "");
    let mine = next_in(&s, None, 0);
    submit(&s, "y", "");
    let start = std::time::Instant::now();
    let theirs = waiter.join().unwrap().unwrap();
    assert!(
        start.elapsed().as_millis() < 1000,
        "the waiting thread was left out"
    );
    match mine {
        Ok(t) => {
            assert_eq!((t.payload.as_str(), theirs.as_str()), ("x", "y"));
            complete(&s, &t);
        }
        Err(_) => assert_eq!(theirs, "x"),
    }
}

#[test]
fn polling_with_short_timeouts_loses_no_task_and_uses_no_release() {
    // Even a pool that allows no release and no retry runs every task once.
    let s = std::sync::Arc::new(new(
        r#"{"pools": [{"name": "default", "max_releases": 0, "retry": {"max_attempts": 1}}]}"#,
    ));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let threads: Vec<_> = (0..6u64)
        .map(|n| {
            let (s, stop) = (s.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut done = 0u32;
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    if let Ok(t) = next_in(&s, None, n % 3) {
                        assert_eq!(t.attempt, 1);
                        complete(&s, &t);
                        done += 1;
                    }
                }
                done
            })
        })
        .collect();
    for i in 0..1500 {
        submit(&s, &i.to_string(), "");
    }
    let snap = eventually(&s, "all done", |v| v["totals"]["succeeded"] == 1500);
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let done: u32 = threads.into_iter().map(|t| t.join().unwrap()).sum();
    assert_eq!(done, 1500);
    assert_eq!(snap["totals"]["releases"], 0);
    assert_eq!(snap["totals"]["dead_lettered"], 0);
}

#[test]
fn a_lease_can_be_reported_during_the_grace_of_a_shutdown() {
    let s = std::sync::Arc::new(new(""));
    submit(&s, "x", "");
    let t = next(&s);
    let stopper = {
        let s = s.clone();
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            assert_eq!(unsafe { cronoq_shutdown(s.0, 10_000) }, CRONOQ_OK);
            start.elapsed()
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(50));
    // No new lease, but the held one is still good.
    assert_eq!(next_in(&s, None, 0).unwrap_err(), CRONOQ_SHUTDOWN);
    assert_eq!(
        unsafe { cronoq_complete(s.0, t.lease, null(), 0) },
        CRONOQ_OK
    );
    // The shutdown does not wait out its grace once everything is reported.
    assert!(stopper.join().unwrap().as_millis() < 5000);
    assert_eq!(
        unsafe { cronoq_complete(s.0, t.lease, null(), 0) },
        CRONOQ_SHUTDOWN
    );
}

#[test]
fn a_failed_call_leaves_a_lease_and_a_buffer_that_are_safe_to_free() {
    let s = new("");
    let mut l = std::mem::MaybeUninit::<cronoq_lease>::uninit();
    unsafe { l.as_mut_ptr().write_bytes(0xAA, 1) };
    assert_eq!(
        unsafe { cronoq_next_lease(s.0, null(), 0, l.as_mut_ptr()) },
        CRONOQ_TIMEOUT
    );
    unsafe { cronoq_lease_free(l.as_mut_ptr()) };
    let mut b = std::mem::MaybeUninit::<cronoq_buf>::uninit();
    unsafe { b.as_mut_ptr().write_bytes(0xAA, 1) };
    assert_eq!(
        unsafe { cronoq_task(s.0, 77, null(), b.as_mut_ptr()) },
        CRONOQ_NOT_FOUND
    );
    unsafe { cronoq_buf_free(b.as_mut_ptr()) };
}

#[cfg(feature = "sqlite")]
#[test]
fn with_a_store_a_held_lease_is_not_a_lost_attempt_and_a_restart_is_quick() {
    let dir = std::env::temp_dir().join(format!("cronoq-ffi-held-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("queue.db");
    let cfg = serde_json::json!({
        "store": {"sqlite": path.to_str().unwrap()},
        "pools": [{"name": "default", "retry": {"max_attempts": 1}}],
    })
    .to_string();
    {
        let s = new(&cfg);
        for i in 0..5 {
            submit(&s, &i.to_string(), "");
        }
        // Two leases are held when the program stops.
        let _a = next(&s);
        let _b = next(&s);
    }
    let start = std::time::Instant::now();
    let s = new(&cfg);
    assert!(
        start.elapsed().as_secs() < 5,
        "reopening took {:?}",
        start.elapsed()
    );
    // All five are still there and none was failed. The two that were held come back after
    // their backoff, as attempt 2 with the first attempt not counted.
    let mut got = Vec::new();
    for _ in 0..5 {
        let t = next(&s);
        assert_eq!(
            t.attempt,
            if t.payload == "0" || t.payload == "1" {
                2
            } else {
                1
            }
        );
        complete(&s, &t);
        got.push(t.payload);
    }
    got.sort();
    assert_eq!(got, ["0", "1", "2", "3", "4"]);
    let snap = eventually(&s, "done", |v| v["totals"]["succeeded"] == 5);
    assert_eq!(snap["totals"]["dead_lettered"], 0);
    drop(s);
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(feature = "sqlite")]
#[test]
fn with_a_store_tasks_survive_a_restart() {
    let dir = std::env::temp_dir().join(format!("cronoq-ffi-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("queue.db");
    let cfg = serde_json::json!({"store": {"sqlite": path.to_str().unwrap()}}).to_string();
    {
        let s = new(&cfg);
        submit(&s, "kept", r#"{"key": "k"}"#);
        submit(&s, "done", "");
        let reason = c("held");
        let key = c("k");
        // Hold the keyed task back so that the other one is the one that runs.
        let _ = key;
        unsafe { cronoq_pause(s.0, null(), -1, null(), reason.as_ptr(), null_mut()) };
    }
    let s = new(&cfg);
    assert_eq!(snapshot(&s)["pools"][0]["paused"], "held");
    unsafe { cronoq_resume(s.0, null(), -1, null(), null_mut()) };
    assert_eq!(drain(&s), ["kept", "done"]);
    drop(s);
    let _ = std::fs::remove_dir_all(dir);
}
