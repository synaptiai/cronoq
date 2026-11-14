package cronoq

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/synaptiai/cronoq/sdks/go/cronoqpb"
)

// Task describes a task to submit. Zero values take the server's defaults. [Task.Spec] gives
// the raw spec, for fields this struct does not cover.
type Task struct {
	Payload     []byte
	ContentType string
	Kind        string // default "default"
	Pool        string // default "default"
	// Key makes the submission idempotent: a task with the same key is returned instead.
	Key      string
	Tenant   string
	Priority Priority
	// Timeout bounds each attempt.
	Timeout time.Duration
	// Deadline is relative to the submission.
	Deadline time.Duration
	// StartIn delays the task.
	StartIn time.Duration
	// After lists the tasks this one waits for.
	After []Dep
	// Resources the task holds while it runs (name to units).
	Resources map[string]uint32
	Tags      map[string]string
	// Labels the task requires of its worker.
	Labels map[string]string
	// MaxAttempts, when set, replaces the pool's retry policy with this many attempts and the
	// server's default backoff.
	MaxAttempts uint32
	// Replace lets the newest submission of Key win. A task with the key that has not
	// started is replaced in place (same id and place in line). One that has started keeps
	// its payload, and this task runs after it as a new task with the same key. See
	// [Submitted.Outcome]. SubmitBatch refuses it; [Task.Spec] does not carry it.
	Replace bool
	// RenewPosition, with Replace, takes this submission's place in line instead of keeping
	// the place of the task it replaces.
	RenewPosition bool
	// RejectConflict reports a Key that belongs to a task submitted with other content
	// (payload, content type, pool, kind, priority or dependencies) as an [*Error] with
	// [Error.KeyConflict] true, instead of returning that task. A retry of the same
	// submission is still answered with the task. In a batch, set it on every task or none.
	RejectConflict bool
	// ReturnExisting returns the task that holds Key whatever it was submitted with, also
	// when the server's default is to report conflicts. Without any of the three options the
	// server's default applies.
	ReturnExisting bool
	// OrderingKey makes tasks of one pool run one at a time, in the order they were
	// submitted, however many workers are free: use it for work that must not overlap or
	// overtake for one subject (an account, a document, a device). Tasks with different keys
	// do not wait for each other. The order inside a key is submission order, not priority;
	// the task at the front runs at the highest class among the key's waiting tasks. It is
	// not a deduplication key (Key): two submissions with the same ordering key are two
	// tasks. Not allowed together with StartIn.
	OrderingKey string
	// HaltOnFailure, with OrderingKey, halts the key if this task ends as failed: the tasks
	// behind it stay queued, in place, until [Client.Resume] or until they are cancelled.
	HaltOnFailure bool
}

// request builds the submission of the task, with its options.
func (t Task) request() (*pb.SubmitRequest, error) {
	req := &pb.SubmitRequest{Task: t.Spec(), RenewPosition: t.RenewPosition}
	chosen := 0
	for _, on := range []bool{t.Replace, t.RejectConflict, t.ReturnExisting} {
		if on {
			chosen++
		}
	}
	switch {
	case chosen > 1:
		return nil, &Error{Code: codes.InvalidArgument, Message: "Replace, RejectConflict and ReturnExisting exclude each other", BatchIndex: -1}
	case t.ReturnExisting:
		req.OnKeyConflict = pb.KeyConflict_KEY_CONFLICT_RETURN_EXISTING
	case t.Replace:
		req.OnKeyConflict = pb.KeyConflict_KEY_CONFLICT_REPLACE_QUEUED
	case t.RejectConflict:
		req.OnKeyConflict = pb.KeyConflict_KEY_CONFLICT_REJECT
	}
	return req, nil
}

// Spec converts the task to its raw protobuf spec.
func (t Task) Spec() *pb.TaskSpec {
	s := &pb.TaskSpec{
		Payload: t.Payload, ContentType: t.ContentType, Kind: t.Kind, Pool: t.Pool, Key: t.Key,
		Tenant: t.Tenant, Priority: t.Priority.pb(), Resources: t.Resources, Tags: t.Tags,
		Labels: t.Labels, OrderingKey: t.OrderingKey,
	}
	if t.HaltOnFailure {
		s.OnFailure = pb.OrderFailure_ORDER_FAILURE_HALT
	}
	if t.Timeout > 0 {
		s.TimeoutS = seconds(t.Timeout)
	}
	if t.Deadline > 0 {
		s.DeadlineInS = seconds(t.Deadline)
	}
	if t.StartIn > 0 {
		s.StartInS = seconds(t.StartIn)
	}
	if t.MaxAttempts > 0 {
		// The server takes zero backoff fields from its defaults; jitter is on by default.
		s.Retry = &pb.RetryPolicy{MaxAttempts: t.MaxAttempts, Jitter: true}
	}
	for _, d := range t.After {
		s.Deps = append(s.Deps, &pb.Dep{Target: d.Ref.pb(), Cond: d.Cond.pb()})
	}
	return s
}

// Dep is a dependency on another task.
type Dep struct {
	Ref  Ref
	Cond Cond
}

// Cond is when a dependency is satisfied.
type Cond int

const (
	// OnSuccess waits for the other task to succeed (the default).
	OnSuccess Cond = iota
	// OnComplete waits for it to finish, whatever its outcome.
	OnComplete
	// OnFailure waits for it to fail.
	OnFailure
)

func (c Cond) pb() pb.EdgeCond {
	switch c {
	case OnComplete:
		return pb.EdgeCond_EDGE_COND_ON_COMPLETE
	case OnFailure:
		return pb.EdgeCond_EDGE_COND_ON_FAILURE
	default:
		return pb.EdgeCond_EDGE_COND_ON_SUCCESS
	}
}

// Priority is a task's priority class: "critical", "high", "normal" (default) or "low".
type Priority string

// Priority classes.
const (
	Critical Priority = "critical"
	High     Priority = "high"
	Normal   Priority = "normal"
	Low      Priority = "low"
)

func (p Priority) pb() pb.Priority {
	return pb.Priority(pb.Priority_value["PRIORITY_"+strings.ToUpper(string(p))])
}

func priority(p pb.Priority) Priority {
	if p == pb.Priority_PRIORITY_UNSPECIFIED {
		return ""
	}
	return Priority(strings.ToLower(strings.TrimPrefix(p.String(), "PRIORITY_")))
}

// Status is a task status, such as "ready", "running" or "succeeded".
type Status string

// Task statuses.
const (
	Blocked      Status = "blocked"
	Scheduled    Status = "scheduled"
	Ready        Status = "ready"
	Running      Status = "running"
	Retrying     Status = "retrying"
	Succeeded    Status = "succeeded"
	Failed       Status = "failed"
	DeadLettered Status = "dead_lettered"
	Cancelled    Status = "cancelled"
	Skipped      Status = "skipped"
	Missed       Status = "missed"
	Shed         Status = "shed"
)

// Done reports whether the status is terminal.
func (s Status) Done() bool {
	switch s {
	case Succeeded, Failed, DeadLettered, Cancelled, Skipped, Missed, Shed:
		return true
	}
	return false
}

func statusOf(s pb.TaskStatus) Status {
	return Status(strings.ToLower(strings.TrimPrefix(s.String(), "TASK_STATUS_")))
}

func (s Status) pb() (pb.TaskStatus, error) {
	v, ok := pb.TaskStatus_value["TASK_STATUS_"+strings.ToUpper(string(s))]
	if !ok || v == 0 {
		return 0, fmt.Errorf("cronoq: unknown status %q", s)
	}
	return pb.TaskStatus(v), nil
}

// TaskInfo is a task's details. Fields other than ID and Status are best effort.
type TaskInfo struct {
	ID              uint64
	Status          Status
	Pool            string
	Key             string
	Kind            string
	Tenant          string
	Priority        Priority
	Attempt         uint32
	MaxAttempts     uint32 // 0 if unknown
	Created         time.Time
	Started         time.Time
	Finished        time.Time
	Message         string
	OutputAvailable bool
}

func unixNs(v *int64) time.Time {
	if v == nil {
		return time.Time{}
	}
	return time.Unix(0, *v)
}

func taskInfo(t *pb.TaskInfo) *TaskInfo {
	return &TaskInfo{
		ID: t.GetId(), Status: statusOf(t.GetStatus()), Pool: t.GetPool(), Key: t.GetKey(),
		Kind: t.GetKind(), Tenant: t.GetTenant(), Priority: priority(t.GetPriority()),
		Attempt: t.GetAttempt(), MaxAttempts: t.GetMaxAttempts(), Created: unixNs(t.CreatedUnixNs),
		Started: unixNs(t.StartedUnixNs), Finished: unixNs(t.FinishedUnixNs),
		Message: t.GetMessage(), OutputAvailable: t.GetOutputAvailable(),
	}
}

// Result is the outcome of a finished task.
type Result struct {
	ID     uint64
	Status Status // always terminal
	Output []byte
	// Message is the last error or status message.
	Message string
	// OutputAvailable is false for a succeeded task whose output is no longer retained.
	OutputAvailable bool
}

// OK reports whether the task succeeded.
func (r *Result) OK() bool { return r.Status == Succeeded }

func result(r *pb.TaskResult) *Result {
	return &Result{
		ID: r.GetId(), Status: statusOf(r.GetStatus()), Output: r.GetOutput(), Message: r.GetMessage(),
		OutputAvailable: r.GetOutputAvailable(),
	}
}

// Error is a failed call.
type Error struct {
	// Code is the gRPC status code.
	Code    codes.Code
	Message string
	// ConflictTask and ConflictStatus name the task that holds the key, when a submission
	// with [Task.RejectConflict] found the key in use by a task with other content.
	ConflictTask   uint64
	ConflictStatus Status
	// BatchIndex is the position of the task that made a batch fail, or -1.
	BatchIndex int
}

// KeyConflict reports whether the submission's key belongs to a task that was submitted
// with other content ([Task.RejectConflict]).
func (e *Error) KeyConflict() bool { return e.ConflictTask != 0 }

func (e *Error) Error() string { return fmt.Sprintf("cronoq: %s: %s", e.Code, e.Message) }

// Retryable reports whether retrying the same call later can succeed (the server was not the
// leader, shut down, or its queue was full).
func (e *Error) Retryable() bool {
	return e.Code == codes.Unavailable || e.Code == codes.ResourceExhausted
}

// Timeout reports whether a wait ended while the task was still running.
func (e *Error) Timeout() bool { return e.Code == codes.DeadlineExceeded }

// NotFound reports whether the task, pool or dependency does not exist (or the caller may
// not see it).
func (e *Error) NotFound() bool { return e.Code == codes.NotFound }

func wrap(err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return err
	}
	if s, ok := status.FromError(err); ok {
		return &Error{Code: s.Code(), Message: s.Message(), BatchIndex: -1}
	}
	return err
}

// wrapWith is wrap for calls whose failure can carry details in the response trailer.
func wrapWith(err error, trailer metadata.MD) error {
	err = wrap(err)
	var e *Error
	if !errors.As(err, &e) {
		return err
	}
	first := func(name string) string {
		if v := trailer.Get(name); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	if id, perr := strconv.ParseUint(first("cronoq-conflict-task"), 10, 64); perr == nil {
		e.ConflictTask = id
		e.ConflictStatus = Status(first("cronoq-conflict-status"))
	}
	if i, perr := strconv.Atoi(first("cronoq-batch-index")); perr == nil {
		e.BatchIndex = i
	}
	return err
}
