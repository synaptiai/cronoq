// Package embedded runs the cronoq scheduler inside a Go program, without a server.
//
// EXPERIMENTAL until cronoq 1.0.
//
// The scheduler lives in the cronoq C library. The program submits tasks (opaque bytes) and
// runs them itself: goroutines take one attempt at a time with [Scheduler.Next], do the
// work, and report the result on the [Lease].
//
// The package has two build modes with the same API and the same behaviour:
//
//   - Default: the dynamic library (libcronoq_ffi.so, libcronoq_ffi.dylib, cronoq_ffi.dll)
//     is loaded at run time, without cgo. It is looked for at the path in the environment
//     variable CRONOQ_LIBRARY, then next to the executable, then by the system's search
//     rules. [Load] loads it from a given path.
//   - With the build tag cronoq_static: the static library is linked in with cgo. Tell the
//     linker where it is, for example CGO_LDFLAGS="-L/path/to/lib".
//
// A [Scheduler] may be used from any number of goroutines at once.
package embedded

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"
)

// ABIVersion is the revision of the C API this package was written for. Opening a
// scheduler fails if the library implements another one.
const ABIVersion = 1

// Statuses of the C API (cronoq_status).
const (
	statusOK              = 0
	statusTimeout         = 1
	statusPaused          = 2
	statusShutdown        = 3
	statusInvalidArgument = 4
	statusNotFound        = 5
	statusLeaseGone       = 6
	statusFull            = 7
	statusConflict        = 8
	statusStore           = 9
)

// Errors that calls return; compare with [errors.Is]. The error of a failed call wraps one
// of these and carries the library's message.
var (
	// ErrTimeout: Next found no attempt in time.
	ErrTimeout = errors.New("cronoq: no task became ready in time")
	// ErrPaused: Next found no attempt in time and the pool is paused. The error is a
	// *PausedError with the reason.
	ErrPaused = errors.New("cronoq: the pool is paused")
	// ErrShutdown: the scheduler was shut down.
	ErrShutdown = errors.New("cronoq: the scheduler was shut down")
	// ErrInvalidArgument: an argument or an option is wrong.
	ErrInvalidArgument = errors.New("cronoq: invalid argument")
	// ErrNotFound: there is no such live task.
	ErrNotFound = errors.New("cronoq: not found")
	// ErrLeaseGone: the lease is over (the attempt timed out, the task was cancelled, or
	// the lease was already ended). Stop working on it.
	ErrLeaseGone = errors.New("cronoq: the lease is over")
	// ErrFull: the pool is full, or a limit of the scheduler is reached.
	ErrFull = errors.New("cronoq: full")
	// ErrConflict: a live task with this key exists and the submission conflicts with it.
	ErrConflict = errors.New("cronoq: key conflict")
	// ErrStore: the store failed.
	ErrStore = errors.New("cronoq: store error")
	// ErrInternal: a fault inside the library.
	ErrInternal = errors.New("cronoq: internal error")
)

// PausedError is returned by Next when the pool is paused. It matches ErrPaused.
type PausedError struct {
	// Reason is the reason the pool was paused with.
	Reason string
}

func (e *PausedError) Error() string        { return "cronoq: the pool is paused: " + e.Reason }
func (e *PausedError) Is(target error) bool { return target == ErrPaused }

func statusError(status int32, message string) error {
	var base error
	switch status {
	case statusOK:
		return nil
	case statusTimeout:
		return ErrTimeout
	case statusPaused:
		return &PausedError{Reason: message}
	case statusShutdown:
		return ErrShutdown
	case statusLeaseGone:
		return ErrLeaseGone
	case statusInvalidArgument:
		base = ErrInvalidArgument
	case statusNotFound:
		base = ErrNotFound
	case statusFull:
		base = ErrFull
	case statusConflict:
		base = ErrConflict
	case statusStore:
		base = ErrStore
	default:
		base = ErrInternal
	}
	if message == "" {
		return base
	}
	return fmt.Errorf("%w: %s", base, message)
}

// Priority is the class of a task.
type Priority string

// The four classes.
const (
	Critical Priority = "critical"
	High     Priority = "high"
	Normal   Priority = "normal"
	Low      Priority = "low"
)

func (p Priority) index() (int32, error) {
	switch p {
	case Critical:
		return 0, nil
	case High:
		return 1, nil
	case Normal:
		return 2, nil
	case Low:
		return 3, nil
	}
	return 0, fmt.Errorf("%w: unknown priority %q", ErrInvalidArgument, string(p))
}

// Retry is a retry policy. Fields left zero keep their default.
type Retry struct {
	// MaxAttempts is the number of attempts in all (default 3).
	MaxAttempts uint32 `json:"max_attempts,omitempty"`
	// InitialBackoffMs is the wait before the second attempt (default 1000).
	InitialBackoffMs uint64 `json:"initial_backoff_ms,omitempty"`
	// Multiplier multiplies the wait after each attempt (default 2).
	Multiplier float64 `json:"multiplier,omitempty"`
	// MaxBackoffMs is the longest wait (default 60000).
	MaxBackoffMs uint64 `json:"max_backoff_ms,omitempty"`
	// Jitter randomises the wait (default true).
	Jitter *bool `json:"jitter,omitempty"`
}

// PerClass holds one value per class; nil keeps the default of that class.
type PerClass struct {
	Critical *float64 `json:"critical,omitempty"`
	High     *float64 `json:"high,omitempty"`
	Normal   *float64 `json:"normal,omitempty"`
	Low      *float64 `json:"low,omitempty"`
}

// PrioritySettings change how classes are weighed and how waiting tasks age.
type PrioritySettings struct {
	Base                *PerClass `json:"base,omitempty"`
	AgingRate           *PerClass `json:"aging_rate,omitempty"`
	AgeCap              *PerClass `json:"age_cap,omitempty"`
	AllowOvertakingHigh *bool     `json:"allow_overtaking_high,omitempty"`
}

// UrgencySettings change how a deadline raises a task.
type UrgencySettings struct {
	Max         *float64 `json:"max,omitempty"`
	WindowS     *float64 `json:"window_s,omitempty"`
	OverdueRate *float64 `json:"overdue_rate,omitempty"`
	OverdueCap  *float64 `json:"overdue_cap,omitempty"`
}

// Pool configures one pool.
type Pool struct {
	Name string `json:"name"`
	// MaxLeases is the most attempts the program may hold at a time (default 256).
	MaxLeases uint32 `json:"max_leases,omitempty"`
	// MaxPending bounds the queue (default: no bound).
	MaxPending uint64 `json:"max_pending,omitempty"`
	// Overflow says what a full queue does: "reject" (default), "shed_lowest" or
	// "shed_oldest".
	Overflow string `json:"overflow,omitempty"`
	Retry    *Retry `json:"retry,omitempty"`
	// MaxReleases is how often an attempt of one task may be released without counting
	// as a failure (default 10).
	MaxReleases *uint32 `json:"max_releases,omitempty"`
	// LeaseTTLMs makes leases expire unless Heartbeat is called within this time.
	LeaseTTLMs uint64            `json:"lease_ttl_ms,omitempty"`
	Priority   *PrioritySettings `json:"priority,omitempty"`
	Urgency    *UrgencySettings  `json:"urgency,omitempty"`
}

// Resource is a limit that tasks share: a semaphore (Kind "semaphore", Permits) or a rate
// limit (Kind "rate", Rate per second and Burst).
type Resource struct {
	Kind    string  `json:"kind"`
	Name    string  `json:"name"`
	Permits uint32  `json:"permits,omitempty"`
	Rate    float64 `json:"rate,omitempty"`
	Burst   uint32  `json:"burst,omitempty"`
}

// Store says where tasks are kept. Without one they are kept in memory.
type Store struct {
	// SQLite is the path of a SQLite file. The library must be built with the SQLite store.
	SQLite string `json:"sqlite,omitempty"`
}

// Config configures a scheduler. The zero value is one pool named "default".
type Config struct {
	Pools     []Pool            `json:"pools,omitempty"`
	Resources []Resource        `json:"resources,omitempty"`
	Priority  *PrioritySettings `json:"priority,omitempty"`
	Urgency   *UrgencySettings  `json:"urgency,omitempty"`
	Store     *Store            `json:"store,omitempty"`
}

// Task holds the options of a submission. The zero value is a normal task in the pool
// "default".
type Task struct {
	Pool     string   `json:"pool,omitempty"`
	Kind     string   `json:"kind,omitempty"`
	Key      string   `json:"key,omitempty"`
	Priority Priority `json:"priority,omitempty"`
	// DeadlineMs is a deadline that far from now.
	DeadlineMs uint64 `json:"deadline_ms,omitempty"`
	// TimeoutMs ends an attempt that runs longer.
	TimeoutMs uint64            `json:"timeout_ms,omitempty"`
	Retry     *Retry            `json:"retry,omitempty"`
	Resources map[string]uint32 `json:"resources,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	// OrderingKey: tasks with one ordering key run one at a time, in submission order.
	OrderingKey string `json:"ordering_key,omitempty"`
	// OnFailure is "continue" (default) or "halt": what a failed task does to the tasks
	// behind it in its ordering key.
	OnFailure string `json:"on_failure,omitempty"`
	// OnKeyConflict is "return_existing" (default), "reject" or "replace_queued": what a
	// submission does when a live task has its key.
	OnKeyConflict string `json:"on_key_conflict,omitempty"`
	// Fingerprint is 1 to 32 hexadecimal digits. With OnKeyConflict "reject", a submission
	// whose key is taken by a task with another fingerprint is a conflict.
	Fingerprint string `json:"fingerprint,omitempty"`
	// RenewPosition makes a replacement go to the back of the queue.
	RenewPosition bool `json:"renew_position,omitempty"`
	// MaxReleases overrides the pool's limit for this task.
	MaxReleases *uint32 `json:"max_releases,omitempty"`
}

// Outcome says what a submission did.
type Outcome int

const (
	// Created: a new task.
	Created Outcome = 0
	// Existing: a live task with this key exists and is returned.
	Existing Outcome = 1
	// Replaced: the queued task with this key was replaced.
	Replaced Outcome = 2
	// Pending: the task replaces a running one when that ends.
	Pending Outcome = 3
)

// Submitted is the result of a submission.
type Submitted struct {
	ID      uint64
	Outcome Outcome
}

// TaskRef names a task: by Key when it is not empty, else by ID.
type TaskRef struct {
	ID  uint64
	Key string
}

// Update changes a queued task. Nil fields are left alone.
type Update struct {
	Priority Priority `json:"priority,omitempty"`
	// DeadlineMs sets a deadline that far from now.
	DeadlineMs *uint64 `json:"deadline_ms,omitempty"`
	// NoDeadline removes the deadline.
	NoDeadline bool `json:"no_deadline,omitempty"`
	// Tags replaces the tags.
	Tags map[string]string `json:"tags,omitempty"`
}

// PauseTarget names what to pause or resume in Pool ("" is "default"): an ordering key if
// OrderingKey is set, else a class if Class is set, else the whole pool.
type PauseTarget struct {
	Pool        string
	Class       Priority
	OrderingKey string
}

// TaskInfo describes a task.
type TaskInfo struct {
	ID       uint64   `json:"id"`
	Key      string   `json:"key"`
	Pool     string   `json:"pool"`
	Kind     string   `json:"kind"`
	Priority Priority `json:"priority"`
	Status   string   `json:"status"`
	Attempt  uint32   `json:"attempt"`
}

// PoolSnapshot is the state of one pool.
type PoolSnapshot struct {
	Name          string              `json:"name"`
	Capacity      uint32              `json:"capacity"`
	Busy          uint32              `json:"busy"`
	Ready         uint64              `json:"ready"`
	Pending       map[Priority]uint64 `json:"pending"`
	Paused        *string             `json:"paused"`
	PausedClasses map[Priority]string `json:"paused_classes"`
	PausedTasks   uint64              `json:"paused_tasks"`
	Ordering      struct {
		Keys    uint64 `json:"keys"`
		Waiting uint64 `json:"waiting"`
		Halted  uint64 `json:"halted"`
		Paused  uint64 `json:"paused"`
	} `json:"ordering"`
}

// Snapshot is the state of the scheduler.
type Snapshot struct {
	Pools     []PoolSnapshot    `json:"pools"`
	LiveTasks uint64            `json:"live_tasks"`
	Running   uint64            `json:"running"`
	Pending   uint64            `json:"pending"`
	Totals    map[string]uint64 `json:"totals"`
}

// Scheduler is an embedded scheduler. Close it when done.
type Scheduler struct {
	mu     sync.RWMutex
	handle unsafe.Pointer
	// calls counts calls in progress, so that Close frees the scheduler only when the
	// library is not in use.
	calls sync.WaitGroup
}

// Version is the version of the loaded library.
func Version() (string, error) {
	if err := ensureLoaded(); err != nil {
		return "", err
	}
	return cVersion(), nil
}

// Open starts a scheduler.
func Open(cfg Config) (*Scheduler, error) {
	if err := ensureLoaded(); err != nil {
		return nil, err
	}
	if got := cABIVersion(); got != ABIVersion {
		return nil, fmt.Errorf("cronoq: the library implements ABI %d, this package needs %d", got, ABIVersion)
	}
	text, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	handle, status, msg := cNew(string(text))
	if err := statusError(status, msg); err != nil {
		return nil, err
	}
	return &Scheduler{handle: handle}, nil
}

// enter marks a call in progress and returns the handle, or ErrShutdown after Close.
func (s *Scheduler) enter() (unsafe.Pointer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.handle == nil {
		return nil, ErrShutdown
	}
	s.calls.Add(1)
	return s.handle, nil
}

// Submit submits a task with payload. opts may be nil.
func (s *Scheduler) Submit(payload []byte, opts *Task) (Submitted, error) {
	h, err := s.enter()
	if err != nil {
		return Submitted{}, err
	}
	defer s.calls.Done()
	options := ""
	if opts != nil {
		text, err := json.Marshal(opts)
		if err != nil {
			return Submitted{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		}
		options = string(text)
	}
	id, outcome, status, msg := cSubmit(h, payload, options)
	if err := statusError(status, msg); err != nil {
		return Submitted{}, err
	}
	return Submitted{ID: id, Outcome: Outcome(outcome)}, nil
}

// Lease is one attempt of a task, taken with Next. End it with exactly one of Complete,
// Fail and Release.
type Lease struct {
	s *Scheduler
	// ID names the attempt.
	ID uint64
	// Task is the task's id.
	Task uint64
	// Attempt is 1 for the first attempt.
	Attempt uint32
	// Payload is what the task was submitted with.
	Payload []byte
	// Key is the task's key, or "".
	Key string
	// Kind is the task's kind, or "".
	Kind string
	// Pool is the task's pool.
	Pool string
	// Tags are the task's tags.
	Tags map[string]string
	// Deadline is the task's deadline, or the zero time.
	Deadline time.Time
	// Timeout is the attempt's timeout, or 0.
	Timeout time.Duration
}

// Next waits up to timeout for an attempt of a task of pool ("" is "default") and takes it.
// The scheduler chooses the task when Next asks, so the order of the queue is decided as
// late as possible.
//
// It returns ErrTimeout when nothing became ready in time, a *PausedError (matching
// ErrPaused) when in addition the pool is paused, and ErrShutdown after Shutdown or Close.
func (s *Scheduler) Next(pool string, timeout time.Duration) (*Lease, error) {
	h, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer s.calls.Done()
	if timeout < 0 {
		timeout = 0
	}
	raw, status, msg := cNextLease(h, pool, uint64(timeout/time.Millisecond))
	if err := statusError(status, msg); err != nil {
		return nil, err
	}
	l := &Lease{s: s, ID: raw.lease, Task: raw.task, Attempt: raw.attempt, Payload: raw.payload, Key: raw.key, Kind: raw.kind}
	var info struct {
		Pool           string            `json:"pool"`
		Tags           map[string]string `json:"tags"`
		DeadlineUnixMs *int64            `json:"deadline_unix_ms"`
		TimeoutMs      *int64            `json:"timeout_ms"`
	}
	if err := json.Unmarshal([]byte(raw.info), &info); err != nil {
		return nil, fmt.Errorf("%w: lease info: %v", ErrInternal, err)
	}
	l.Pool, l.Tags = info.Pool, info.Tags
	if info.DeadlineUnixMs != nil {
		l.Deadline = time.UnixMilli(*info.DeadlineUnixMs)
	}
	if info.TimeoutMs != nil {
		l.Timeout = time.Duration(*info.TimeoutMs) * time.Millisecond
	}
	return l, nil
}

func (l *Lease) end(f func(h unsafe.Pointer) (int32, string)) error {
	h, err := l.s.enter()
	if err != nil {
		return err
	}
	defer l.s.calls.Done()
	return statusError(f(h))
}

// Complete reports that the attempt succeeded.
func (l *Lease) Complete() error {
	return l.end(func(h unsafe.Pointer) (int32, string) { return cComplete(h, l.ID) })
}

// Fail reports that the attempt failed. A retryable failure is tried again after the
// task's backoff, and not before retryAfter has passed if that is positive, until the
// retry policy is used up. A failure that is not retryable ends the task.
func (l *Lease) Fail(message string, retryable bool, retryAfter time.Duration) error {
	after := int64(-1)
	if retryAfter > 0 {
		after = int64(retryAfter / time.Millisecond)
	}
	return l.end(func(h unsafe.Pointer) (int32, string) { return cFail(h, l.ID, message, retryable, after) })
}

// Release gives the attempt back without a verdict: the program cannot run the task now,
// and nothing is wrong with the task. The task keeps its place and is handed out again
// after delay. This does not use up a retry, up to the pool's or the task's MaxReleases.
func (l *Lease) Release(delay time.Duration) error {
	if delay < 0 {
		delay = 0
	}
	return l.end(func(h unsafe.Pointer) (int32, string) { return cRelease(h, l.ID, uint64(delay/time.Millisecond)) })
}

// Heartbeat extends the lease (needed for pools with LeaseTTLMs). ErrLeaseGone means the
// lease is over, for example because its task was cancelled: stop working on it.
func (l *Lease) Heartbeat() error {
	return l.end(func(h unsafe.Pointer) (int32, string) { return cHeartbeat(h, l.ID) })
}

// Cancel cancels a task. It reports whether a live task was cancelled.
func (s *Scheduler) Cancel(ref TaskRef) (bool, error) {
	h, err := s.enter()
	if err != nil {
		return false, err
	}
	defer s.calls.Done()
	changed, status, msg := cCancel(h, ref.ID, ref.Key)
	return changed, statusError(status, msg)
}

// Update changes a queued task without losing its place or its age. It reports whether
// something changed.
func (s *Scheduler) Update(ref TaskRef, u Update) (bool, error) {
	h, err := s.enter()
	if err != nil {
		return false, err
	}
	defer s.calls.Done()
	text, err := json.Marshal(u)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	changed, status, msg := cUpdate(h, ref.ID, ref.Key, string(text))
	return changed, statusError(status, msg)
}

// SetOrderingFloor sets the lowest class the tasks of an ordering key run at; an empty
// floor removes it. With persist the floor stays when the key has no tasks left.
func (s *Scheduler) SetOrderingFloor(pool, orderingKey string, floor Priority, persist bool) error {
	h, err := s.enter()
	if err != nil {
		return err
	}
	defer s.calls.Done()
	class := int32(-1)
	if floor != "" {
		if class, err = floor.index(); err != nil {
			return err
		}
	}
	return statusError(cSetOrderingFloor(h, pool, orderingKey, class, persist))
}

func (t PauseTarget) class() (int32, error) {
	if t.Class == "" {
		return -1, nil
	}
	return t.Class.index()
}

// Pause holds back a pool, one class of it or one ordering key of it: its tasks stay
// queued and keep their place, and none is handed out until Resume. The reason is required.
// It reports whether this paused something that was not paused.
func (s *Scheduler) Pause(target PauseTarget, reason string) (bool, error) {
	h, err := s.enter()
	if err != nil {
		return false, err
	}
	defer s.calls.Done()
	class, err := target.class()
	if err != nil {
		return false, err
	}
	changed, status, msg := cPause(h, target.Pool, class, target.OrderingKey, reason)
	return changed, statusError(status, msg)
}

// Resume ends a pause. Resuming an ordering key also resumes a key that a failed task
// halted. It reports whether something was resumed.
func (s *Scheduler) Resume(target PauseTarget) (bool, error) {
	h, err := s.enter()
	if err != nil {
		return false, err
	}
	defer s.calls.Done()
	class, err := target.class()
	if err != nil {
		return false, err
	}
	changed, status, msg := cResume(h, target.Pool, class, target.OrderingKey)
	return changed, statusError(status, msg)
}

// Task describes a live task. It returns ErrNotFound if the scheduler does not know it.
func (s *Scheduler) Task(ref TaskRef) (*TaskInfo, error) {
	h, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer s.calls.Done()
	text, status, msg := cTask(h, ref.ID, ref.Key)
	if err := statusError(status, msg); err != nil {
		return nil, err
	}
	var info TaskInfo
	if err := json.Unmarshal([]byte(text), &info); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInternal, err)
	}
	return &info, nil
}

// Snapshot returns the state of the scheduler.
func (s *Scheduler) Snapshot() (*Snapshot, error) {
	h, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer s.calls.Done()
	text, status, msg := cSnapshot(h)
	if err := statusError(status, msg); err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal([]byte(text), &snap); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInternal, err)
	}
	return &snap, nil
}

// Shutdown stops the scheduler: goroutines waiting in Next return ErrShutdown at once and
// no new lease is handed out. For up to grace, leases that are held can still be reported;
// Shutdown returns as soon as all are. A lease that is still held after that is released:
// its task keeps its place and no retry is used. Close still has to be called.
func (s *Scheduler) Shutdown(grace time.Duration) error {
	h, err := s.enter()
	if err != nil {
		return nil
	}
	defer s.calls.Done()
	if grace < 0 {
		grace = 0
	}
	return statusError(cShutdown(h, uint64(grace/time.Millisecond)))
}

// Close stops the scheduler if Shutdown was not called, waits for calls in progress to
// return, and frees it. Calling it twice is allowed.
func (s *Scheduler) Close() {
	_ = s.Shutdown(0)
	s.mu.Lock()
	h := s.handle
	s.handle = nil
	s.mu.Unlock()
	if h == nil {
		return
	}
	s.calls.Wait()
	cFree(h)
}
