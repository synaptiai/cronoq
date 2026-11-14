//go:build cronoq_static

package embedded

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo linux LDFLAGS: -lcronoq_ffi -lpthread -ldl -lm
#cgo darwin LDFLAGS: -lcronoq_ffi -lm
#cgo windows LDFLAGS: -lcronoq_ffi -lws2_32 -luserenv -lntdll -lbcrypt -ladvapi32 -lkernel32
#include <stdlib.h>
#include "cronoq.h"
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// Load does nothing in this build mode: the library is linked in.
func Load(path string) error { return nil }

func ensureLoaded() error { return nil }

// cstr is s as a C string, to be freed by the caller. An empty string with null set is a
// null pointer.
func cstr(s string, null bool) *C.char {
	if s == "" && null {
		return nil
	}
	return C.CString(s)
}

func free(ps ...*C.char) {
	for _, p := range ps {
		if p != nil {
			C.free(unsafe.Pointer(p))
		}
	}
}

func sched(h unsafe.Pointer) *C.cronoq_scheduler {
	return (*C.cronoq_scheduler)(h)
}

// call runs f on one OS thread and reads the library's message for a failed call from the
// same thread: the message is kept per thread, and a goroutine may move between threads.
func call(f func() int32) (int32, string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	status := f()
	if status == statusOK || status == statusTimeout || status == statusLeaseGone {
		return status, ""
	}
	return status, C.GoString(C.cronoq_last_error())
}

func cABIVersion() uint32 { return uint32(C.cronoq_abi_version()) }
func cVersion() string    { return C.GoString(C.cronoq_version()) }

func cNew(config string) (unsafe.Pointer, int32, string) {
	cfg := cstr(config, false)
	defer free(cfg)
	var out *C.cronoq_scheduler
	status, msg := call(func() int32 { return int32(C.cronoq_new(cfg, &out)) })
	return unsafe.Pointer(out), status, msg
}

func cShutdown(h unsafe.Pointer, graceMs uint64) (int32, string) {
	return call(func() int32 { return int32(C.cronoq_shutdown(sched(h), C.uint64_t(graceMs))) })
}

func cFree(h unsafe.Pointer) { C.cronoq_free(sched(h)) }

func cSubmit(h unsafe.Pointer, payload []byte, options string) (uint64, int32, int32, string) {
	opts := cstr(options, true)
	defer free(opts)
	var (
		id      C.uint64_t
		outcome C.int32_t
		p       *C.uint8_t
	)
	if len(payload) > 0 {
		p = (*C.uint8_t)(unsafe.Pointer(&payload[0]))
	}
	status, msg := call(func() int32 {
		return int32(C.cronoq_submit(sched(h), p, C.size_t(len(payload)), opts, &id, &outcome))
	})
	return uint64(id), int32(outcome), status, msg
}

// rawLease is a lease copied out of the library's memory.
type rawLease struct {
	lease, task uint64
	attempt     uint32
	payload     []byte
	key, kind   string
	info        string
}

func cNextLease(h unsafe.Pointer, pool string, timeoutMs uint64) (rawLease, int32, string) {
	p := cstr(pool, true)
	defer free(p)
	var l C.cronoq_lease
	status, msg := call(func() int32 {
		return int32(C.cronoq_next_lease(sched(h), p, C.uint64_t(timeoutMs), &l))
	})
	if status != statusOK {
		return rawLease{}, status, msg
	}
	raw := rawLease{lease: uint64(l.lease), task: uint64(l.task), attempt: uint32(l.attempt), kind: C.GoString(l.kind), info: C.GoString(l.info_json)}
	if l.key != nil {
		raw.key = C.GoString(l.key)
	}
	if l.payload_len > 0 {
		raw.payload = C.GoBytes(unsafe.Pointer(l.payload), C.int(l.payload_len))
	}
	C.cronoq_lease_free(&l)
	return raw, status, ""
}

func cComplete(h unsafe.Pointer, lease uint64) (int32, string) {
	return call(func() int32 { return int32(C.cronoq_complete(sched(h), C.uint64_t(lease))) })
}

func b2i(b bool) C.int32_t {
	if b {
		return 1
	}
	return 0
}

func cFail(h unsafe.Pointer, lease uint64, message string, retryable bool, retryAfterMs int64) (int32, string) {
	m := cstr(message, true)
	defer free(m)
	return call(func() int32 {
		return int32(C.cronoq_fail(sched(h), C.uint64_t(lease), m, b2i(retryable), C.int64_t(retryAfterMs)))
	})
}

func cRelease(h unsafe.Pointer, lease uint64, delayMs uint64) (int32, string) {
	return call(func() int32 {
		return int32(C.cronoq_release(sched(h), C.uint64_t(lease), C.uint64_t(delayMs)))
	})
}

func cHeartbeat(h unsafe.Pointer, lease uint64) (int32, string) {
	return call(func() int32 { return int32(C.cronoq_heartbeat(sched(h), C.uint64_t(lease))) })
}

func cCancel(h unsafe.Pointer, id uint64, key string) (bool, int32, string) {
	k := cstr(key, true)
	defer free(k)
	var out C.int32_t
	status, msg := call(func() int32 { return int32(C.cronoq_cancel(sched(h), C.uint64_t(id), k, &out)) })
	return out != 0, status, msg
}

func cUpdate(h unsafe.Pointer, id uint64, key string, update string) (bool, int32, string) {
	k, u := cstr(key, true), cstr(update, false)
	defer free(k, u)
	var out C.int32_t
	status, msg := call(func() int32 { return int32(C.cronoq_update(sched(h), C.uint64_t(id), k, u, &out)) })
	return out != 0, status, msg
}

func cSetOrderingFloor(h unsafe.Pointer, pool, key string, floor int32, persist bool) (int32, string) {
	p, k := cstr(pool, true), cstr(key, false)
	defer free(p, k)
	return call(func() int32 {
		return int32(C.cronoq_set_ordering_floor(sched(h), p, k, C.int32_t(floor), b2i(persist)))
	})
}

func cPause(h unsafe.Pointer, pool string, class int32, key, reason string) (bool, int32, string) {
	p, k, r := cstr(pool, true), cstr(key, true), cstr(reason, false)
	defer free(p, k, r)
	var out C.int32_t
	status, msg := call(func() int32 {
		return int32(C.cronoq_pause(sched(h), p, C.int32_t(class), k, r, &out))
	})
	return out != 0, status, msg
}

func cResume(h unsafe.Pointer, pool string, class int32, key string) (bool, int32, string) {
	p, k := cstr(pool, true), cstr(key, true)
	defer free(p, k)
	var out C.int32_t
	status, msg := call(func() int32 { return int32(C.cronoq_resume(sched(h), p, C.int32_t(class), k, &out)) })
	return out != 0, status, msg
}

func takeBuf(b *C.cronoq_buf) string {
	s := ""
	if b.ptr != nil {
		s = C.GoStringN((*C.char)(unsafe.Pointer(b.ptr)), C.int(b.len))
	}
	C.cronoq_buf_free(b)
	return s
}

func cTask(h unsafe.Pointer, id uint64, key string) (string, int32, string) {
	k := cstr(key, true)
	defer free(k)
	var b C.cronoq_buf
	status, msg := call(func() int32 { return int32(C.cronoq_task(sched(h), C.uint64_t(id), k, &b)) })
	if status != statusOK {
		return "", status, msg
	}
	return takeBuf(&b), status, ""
}

func cSnapshot(h unsafe.Pointer) (string, int32, string) {
	var b C.cronoq_buf
	status, msg := call(func() int32 { return int32(C.cronoq_snapshot(sched(h), &b)) })
	if status != statusOK {
		return "", status, msg
	}
	return takeBuf(&b), status, ""
}
