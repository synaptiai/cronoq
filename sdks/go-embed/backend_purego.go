//go:build !cronoq_static

package embedded

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// The C functions, bound when the library is loaded.
var (
	fnABIVersion       func() uint32
	fnVersion          func() *byte
	fnLastError        func() *byte
	fnNew              func(config *byte, out *unsafe.Pointer) int32
	fnShutdown         func(s unsafe.Pointer, graceMs uint64) int32
	fnFree             func(s unsafe.Pointer)
	fnSubmit           func(s unsafe.Pointer, payload *byte, n uintptr, options *byte, outTask *uint64, outOutcome *int32) int32
	fnNextLease        func(s unsafe.Pointer, pool *byte, timeoutMs uint64, out *cLease) int32
	fnLeaseFree        func(l *cLease)
	fnComplete         func(s unsafe.Pointer, lease uint64, output *byte, n uintptr) int32
	fnWait             func(s unsafe.Pointer, task uint64, key *byte, timeoutMs uint64, ok *int32, out *cBuf) int32
	fnEventsOpen       func(s unsafe.Pointer, out *unsafe.Pointer) int32
	fnEventsNext       func(e unsafe.Pointer, timeoutMs uint64, out *cBuf) int32
	fnEventsClose      func(e unsafe.Pointer)
	fnFail             func(s unsafe.Pointer, lease uint64, message *byte, retryable int32, retryAfterMs int64) int32
	fnRelease          func(s unsafe.Pointer, lease uint64, delayMs uint64) int32
	fnHeartbeat        func(s unsafe.Pointer, lease uint64) int32
	fnCancel           func(s unsafe.Pointer, task uint64, key *byte, out *int32) int32
	fnUpdate           func(s unsafe.Pointer, task uint64, key *byte, update *byte, out *int32) int32
	fnSetOrderingFloor func(s unsafe.Pointer, pool *byte, key *byte, floor int32, persist int32) int32
	fnPause            func(s unsafe.Pointer, pool *byte, class int32, key *byte, reason *byte, out *int32) int32
	fnResume           func(s unsafe.Pointer, pool *byte, class int32, key *byte, out *int32) int32
	fnTask             func(s unsafe.Pointer, task uint64, key *byte, out *cBuf) int32
	fnSnapshot         func(s unsafe.Pointer, out *cBuf) int32
	fnBufFree          func(b *cBuf)
	fnResyncClock      func(s unsafe.Pointer, out *uint64) int32
	fnAdvanceClock     func(s unsafe.Pointer, byMs uint64) int32
)

// cLease mirrors struct cronoq_lease.
type cLease struct {
	lease      uint64
	task       uint64
	attempt    uint32
	payload    unsafe.Pointer
	payloadLen uintptr
	key        *byte
	kind       *byte
	infoJSON   *byte
	owner      unsafe.Pointer
}

// cBuf mirrors struct cronoq_buf.
type cBuf struct {
	ptr *byte
	len uintptr
}

var (
	loadMu  sync.Mutex
	loaded  bool
	loadErr error
)

func libraryName() string {
	switch runtime.GOOS {
	case "windows":
		return "cronoq_ffi.dll"
	case "darwin":
		return "libcronoq_ffi.dylib"
	default:
		return "libcronoq_ffi.so"
	}
}

// Load loads the cronoq dynamic library from path. Without it, the library is loaded on
// first use from the path in CRONOQ_LIBRARY, else from the directory of the executable,
// else by the system's search rules. Loading twice is an error only if the first load
// failed. With the build tag cronoq_static there is nothing to load and Load does nothing.
func Load(path string) error {
	loadMu.Lock()
	defer loadMu.Unlock()
	if loaded {
		return nil
	}
	handle, err := openLibrary(path)
	if err != nil {
		return fmt.Errorf("cronoq: loading %s: %w", path, err)
	}
	if err := bind(handle); err != nil {
		return err
	}
	loaded = true
	return nil
}

// bind looks up every function. A missing one is an error, not a crash at the first call.
func bind(handle uintptr) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cronoq: the library is not a cronoq library of this version: %v", r)
		}
	}()
	for name, fn := range map[string]any{
		"cronoq_abi_version":        &fnABIVersion,
		"cronoq_version":            &fnVersion,
		"cronoq_last_error":         &fnLastError,
		"cronoq_new":                &fnNew,
		"cronoq_shutdown":           &fnShutdown,
		"cronoq_free":               &fnFree,
		"cronoq_submit":             &fnSubmit,
		"cronoq_next_lease":         &fnNextLease,
		"cronoq_lease_free":         &fnLeaseFree,
		"cronoq_complete":           &fnComplete,
		"cronoq_wait":               &fnWait,
		"cronoq_events_open":        &fnEventsOpen,
		"cronoq_events_next":        &fnEventsNext,
		"cronoq_events_close":       &fnEventsClose,
		"cronoq_fail":               &fnFail,
		"cronoq_release":            &fnRelease,
		"cronoq_heartbeat":          &fnHeartbeat,
		"cronoq_cancel":             &fnCancel,
		"cronoq_update":             &fnUpdate,
		"cronoq_set_ordering_floor": &fnSetOrderingFloor,
		"cronoq_pause":              &fnPause,
		"cronoq_resume":             &fnResume,
		"cronoq_task":               &fnTask,
		"cronoq_snapshot":           &fnSnapshot,
		"cronoq_buf_free":           &fnBufFree,
		"cronoq_resync_clock":       &fnResyncClock,
		"cronoq_advance_clock":      &fnAdvanceClock,
	} {
		purego.RegisterLibFunc(fn, handle, name)
	}
	return nil
}

func ensureLoaded() error {
	loadMu.Lock()
	if loaded || loadErr != nil {
		defer loadMu.Unlock()
		return loadErr
	}
	loadMu.Unlock()
	var candidates []string
	if p := os.Getenv("CRONOQ_LIBRARY"); p != "" {
		candidates = []string{p}
	} else {
		if exe, err := os.Executable(); err == nil {
			candidates = append(candidates, filepath.Join(filepath.Dir(exe), libraryName()))
		}
		candidates = append(candidates, libraryName())
	}
	var err error
	for _, c := range candidates {
		if err = Load(c); err == nil {
			return nil
		}
	}
	loadMu.Lock()
	defer loadMu.Unlock()
	if !loaded {
		loadErr = fmt.Errorf("%w (set CRONOQ_LIBRARY to the path of %s, or call Load)", err, libraryName())
	}
	return loadErr
}

// cstr is s as a C string. An empty string with null set is a null pointer.
func cstr(s string, null bool) *byte {
	if s == "" && null {
		return nil
	}
	b := append([]byte(s), 0)
	return &b[0]
}

func gostr(p *byte) string {
	if p == nil {
		return ""
	}
	n := 0
	for *(*byte)(unsafe.Add(unsafe.Pointer(p), n)) != 0 {
		n++
	}
	return string(unsafe.Slice(p, n))
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
	return status, gostr(fnLastError())
}

func cABIVersion() uint32 { return fnABIVersion() }
func cVersion() string    { return gostr(fnVersion()) }

func cNew(config string) (unsafe.Pointer, int32, string) {
	var h unsafe.Pointer
	status, msg := call(func() int32 { return fnNew(cstr(config, false), &h) })
	return h, status, msg
}

func cShutdown(h unsafe.Pointer, graceMs uint64) (int32, string) {
	return call(func() int32 { return fnShutdown(h, graceMs) })
}

func cFree(h unsafe.Pointer) { fnFree(h) }

func cSubmit(h unsafe.Pointer, payload []byte, options string) (uint64, int32, int32, string) {
	var (
		id      uint64
		outcome int32
		p       *byte
	)
	if len(payload) > 0 {
		p = &payload[0]
	}
	status, msg := call(func() int32 {
		return fnSubmit(h, p, uintptr(len(payload)), cstr(options, true), &id, &outcome)
	})
	runtime.KeepAlive(payload)
	return id, outcome, status, msg
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
	var l cLease
	status, msg := call(func() int32 { return fnNextLease(h, cstr(pool, true), timeoutMs, &l) })
	if status != statusOK {
		return rawLease{}, status, msg
	}
	raw := rawLease{lease: l.lease, task: l.task, attempt: l.attempt, key: gostr(l.key), kind: gostr(l.kind), info: gostr(l.infoJSON)}
	if l.payloadLen > 0 {
		raw.payload = append([]byte(nil), unsafe.Slice((*byte)(l.payload), int(l.payloadLen))...)
	}
	fnLeaseFree(&l)
	return raw, status, ""
}

func cComplete(h unsafe.Pointer, lease uint64, output []byte) (int32, string) {
	var p *byte
	if len(output) > 0 {
		p = &output[0]
	}
	status, msg := call(func() int32 { return fnComplete(h, lease, p, uintptr(len(output))) })
	runtime.KeepAlive(output)
	return status, msg
}

func cWait(h unsafe.Pointer, id uint64, key string, timeoutMs uint64) (bool, []byte, int32, string) {
	var (
		ok int32
		b  cBuf
	)
	status, msg := call(func() int32 { return fnWait(h, id, cstr(key, true), timeoutMs, &ok, &b) })
	if status != statusOK {
		fnBufFree(&b)
		return false, nil, status, msg
	}
	var data []byte
	if b.ptr != nil {
		data = append([]byte(nil), unsafe.Slice(b.ptr, int(b.len))...)
	}
	fnBufFree(&b)
	return ok == 1, data, status, ""
}

func cEventsOpen(h unsafe.Pointer) (unsafe.Pointer, int32, string) {
	var e unsafe.Pointer
	status, msg := call(func() int32 { return fnEventsOpen(h, &e) })
	return e, status, msg
}

func cEventsNext(e unsafe.Pointer, timeoutMs uint64) (string, int32, string) {
	var b cBuf
	status, msg := call(func() int32 { return fnEventsNext(e, timeoutMs, &b) })
	if status != statusOK {
		fnBufFree(&b)
		return "", status, msg
	}
	return takeBuf(&b), status, ""
}

func cEventsClose(e unsafe.Pointer) { fnEventsClose(e) }

func b2i(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

func cFail(h unsafe.Pointer, lease uint64, message string, retryable bool, retryAfterMs int64) (int32, string) {
	return call(func() int32 { return fnFail(h, lease, cstr(message, true), b2i(retryable), retryAfterMs) })
}

func cRelease(h unsafe.Pointer, lease uint64, delayMs uint64) (int32, string) {
	return call(func() int32 { return fnRelease(h, lease, delayMs) })
}

func cHeartbeat(h unsafe.Pointer, lease uint64) (int32, string) {
	return call(func() int32 { return fnHeartbeat(h, lease) })
}

func cCancel(h unsafe.Pointer, id uint64, key string) (bool, int32, string) {
	var out int32
	status, msg := call(func() int32 { return fnCancel(h, id, cstr(key, true), &out) })
	return out != 0, status, msg
}

func cUpdate(h unsafe.Pointer, id uint64, key string, update string) (bool, int32, string) {
	var out int32
	status, msg := call(func() int32 { return fnUpdate(h, id, cstr(key, true), cstr(update, false), &out) })
	return out != 0, status, msg
}

func cSetOrderingFloor(h unsafe.Pointer, pool, key string, floor int32, persist bool) (int32, string) {
	return call(func() int32 {
		return fnSetOrderingFloor(h, cstr(pool, true), cstr(key, false), floor, b2i(persist))
	})
}

func cPause(h unsafe.Pointer, pool string, class int32, key, reason string) (bool, int32, string) {
	var out int32
	status, msg := call(func() int32 {
		return fnPause(h, cstr(pool, true), class, cstr(key, true), cstr(reason, false), &out)
	})
	return out != 0, status, msg
}

func cResume(h unsafe.Pointer, pool string, class int32, key string) (bool, int32, string) {
	var out int32
	status, msg := call(func() int32 { return fnResume(h, cstr(pool, true), class, cstr(key, true), &out) })
	return out != 0, status, msg
}

func takeBuf(b *cBuf) string {
	s := ""
	if b.ptr != nil {
		s = string(unsafe.Slice(b.ptr, int(b.len)))
	}
	fnBufFree(b)
	return s
}

func cTask(h unsafe.Pointer, id uint64, key string) (string, int32, string) {
	var b cBuf
	status, msg := call(func() int32 { return fnTask(h, id, cstr(key, true), &b) })
	if status != statusOK {
		return "", status, msg
	}
	return takeBuf(&b), status, ""
}

func cSnapshot(h unsafe.Pointer) (string, int32, string) {
	var b cBuf
	status, msg := call(func() int32 { return fnSnapshot(h, &b) })
	if status != statusOK {
		return "", status, msg
	}
	return takeBuf(&b), status, ""
}

func cResyncClock(h unsafe.Pointer) (uint64, int32, string) {
	var moved uint64
	status, msg := call(func() int32 { return fnResyncClock(h, &moved) })
	return moved, status, msg
}

func cAdvanceClock(h unsafe.Pointer, byMs uint64) (int32, string) {
	return call(func() int32 { return fnAdvanceClock(h, byMs) })
}
