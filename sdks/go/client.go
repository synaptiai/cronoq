// Package cronoq is the Go client for a cronoq server.
//
// [Dial] returns a [Client] that submits, inspects, waits for, cancels and watches tasks.
// [RunWorker] serves a pool: it leases tasks from the server and runs a [Handler] for each,
// with heartbeats, reconnects and graceful draining.
//
//	c, err := cronoq.Dial("http://127.0.0.1:7070", cronoq.Options{Token: os.Getenv("CRONOQ_TOKEN")})
//	if err != nil { ... }
//	defer c.Close()
//	sub, err := c.Submit(ctx, cronoq.Task{Payload: []byte("hello"), Key: "greeting"})
//	res, err := c.Wait(ctx, cronoq.ID(sub.ID), 30*time.Second)
//
// The generated gRPC stubs are in the cronoqpb package, for anything this package does not
// wrap.
package cronoq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/synaptiai/cronoq/sdks/go/cronoqpb"
)

// ProtocolVersion is the worker protocol version this package speaks.
const ProtocolVersion = 3

// Options configure a connection to a server.
type Options struct {
	// Token is the bearer token sent with every call.
	Token string
	// TLS configures https:// targets (nil: the system roots). It needs an https:// target.
	TLS *tls.Config
	// MaxMessageBytes caps messages received from the server (default 64 MiB).
	MaxMessageBytes int
	// DialOptions are added to the gRPC dial options.
	DialOptions []grpc.DialOption
	// Logger receives warnings (default slog.Default()).
	Logger *slog.Logger
}

func (o *Options) logger() *slog.Logger {
	if o.Logger != nil {
		return o.Logger
	}
	return slog.Default()
}

// Client calls the Tasks and Admin services of one server.
type Client struct {
	conn  *grpc.ClientConn
	tasks pb.TasksClient
	admin pb.AdminClient
}

// Dial connects to the server at target: "http://host:port", "https://host:port" or
// "host:port" (plain). The connection is opened lazily, on the first call.
func Dial(target string, opts Options) (*Client, error) {
	conn, err := dial(target, opts)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, tasks: pb.NewTasksClient(conn), admin: pb.NewAdminClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Conn is the underlying gRPC connection, for the generated stubs in cronoqpb.
func (c *Client) Conn() *grpc.ClientConn { return c.conn }

// endpoint splits a target into the address to dial and whether it uses TLS.
func endpoint(target string) (addr string, secure bool, err error) {
	switch {
	case strings.HasPrefix(target, "https://"):
		addr, secure = strings.TrimPrefix(target, "https://"), true
	case strings.HasPrefix(target, "http://"):
		addr = strings.TrimPrefix(target, "http://")
	case strings.Contains(target, "://"):
		return "", false, fmt.Errorf("cronoq: unsupported target %q (use http:// or https://)", target)
	default:
		addr = target
	}
	addr = strings.TrimSuffix(addr, "/")
	if strings.ContainsAny(addr, "@/?#") {
		return "", false, fmt.Errorf("cronoq: target %q must be a host and port", target)
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return "", false, fmt.Errorf("cronoq: target %q: %w", target, err)
	}
	return addr, secure, nil
}

// isLoopback reports whether the target's host is localhost or a loopback address.
func isLoopback(target string) bool {
	addr, _, err := endpoint(target)
	if err != nil {
		return false
	}
	host, _, _ := net.SplitHostPort(addr)
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type bearer struct{ header string }

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": b.header}, nil
}

// RequireTransportSecurity is false so that loopback and trusted-network servers work over
// plain http; Dial warns when a token goes over plain http to another host.
func (bearer) RequireTransportSecurity() bool { return false }

func dial(target string, opts Options) (*grpc.ClientConn, error) {
	addr, secure, err := endpoint(target)
	if err != nil {
		return nil, err
	}
	if !secure && opts.TLS != nil {
		return nil, fmt.Errorf("cronoq: TLS settings need an https:// target, not %q", target)
	}
	max := opts.MaxMessageBytes
	if max <= 0 {
		max = 64 << 20
	}
	dopts := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(max), grpc.MaxCallSendMsgSize(max))}
	if secure {
		cfg := opts.TLS
		if cfg == nil {
			cfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		dopts = append(dopts, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	} else {
		dopts = append(dopts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if opts.Token != "" {
		if !secure && !isLoopback(target) {
			opts.logger().Warn("cronoq: the token is sent over plain http; use https outside a trusted network", "target", target)
		}
		dopts = append(dopts, grpc.WithPerRPCCredentials(bearer{"Bearer " + opts.Token}))
	}
	dopts = append(dopts, opts.DialOptions...)
	return grpc.NewClient(addr, dopts...)
}

// Submitted is the answer to a submission.
type Submitted struct {
	// ID of the task, or of the existing task with the same key.
	ID uint64
	// Duplicate is true when a task with the same key already existed.
	Duplicate bool
	// Result is set by SubmitAndWait.
	Result *Result
	// Outcome says what the submission did.
	Outcome Outcome
}

// Outcome is what an accepted submission did.
type Outcome string

const (
	// OutcomeCreated: a new task was created.
	OutcomeCreated Outcome = "created"
	// OutcomeExisting: the key was in use, and that task is returned unchanged.
	OutcomeExisting Outcome = "existing"
	// OutcomeReplaced: the task holding the key had not started and now carries this submission
	// ([Task.Replace]).
	OutcomeReplaced Outcome = "replaced"
	// OutcomePending: the task holding the key has started, so this submission is stored and runs
	// after it as a new task with the same key. Submitted.ID, and Result after
	// SubmitAndWait, are the running task's.
	OutcomePending Outcome = "pending"
)

func submitted(r *pb.SubmitResponse) Submitted {
	s := Submitted{ID: r.GetId(), Duplicate: r.GetDuplicate()}
	switch r.GetOutcome() {
	case pb.SubmitOutcome_SUBMIT_OUTCOME_CREATED:
		s.Outcome = OutcomeCreated
	case pb.SubmitOutcome_SUBMIT_OUTCOME_EXISTING:
		s.Outcome = OutcomeExisting
	case pb.SubmitOutcome_SUBMIT_OUTCOME_REPLACED:
		s.Outcome = OutcomeReplaced
	case pb.SubmitOutcome_SUBMIT_OUTCOME_PENDING:
		s.Outcome = OutcomePending
	default:
		// A server older than the field only says whether the key existed.
		s.Outcome = OutcomeCreated
		if s.Duplicate {
			s.Outcome = OutcomeExisting
		}
	}
	if r.Result != nil {
		s.Result = result(r.Result)
	}
	return s
}

// Submit submits one task.
func (c *Client) Submit(ctx context.Context, t Task) (Submitted, error) {
	req, err := t.request()
	if err != nil {
		return Submitted{}, err
	}
	return c.submit(ctx, req, false, 0)
}

// SubmitAndWait submits one task and waits up to timeout (0: the server's limit) for it to
// finish. When it is still running at the timeout, the error is a [*Error] with
// [Error.Timeout] true; the task stays submitted.
func (c *Client) SubmitAndWait(ctx context.Context, t Task, timeout time.Duration) (Submitted, error) {
	req, err := t.request()
	if err != nil {
		return Submitted{}, err
	}
	return c.submit(ctx, req, true, timeout)
}

// SubmitSpec submits a task described by a raw spec.
func (c *Client) SubmitSpec(ctx context.Context, spec *pb.TaskSpec, wait bool, timeout time.Duration) (Submitted, error) {
	return c.submit(ctx, &pb.SubmitRequest{Task: spec}, wait, timeout)
}

func (c *Client) submit(ctx context.Context, req *pb.SubmitRequest, wait bool, timeout time.Duration) (Submitted, error) {
	req.Wait = wait
	if wait && timeout > 0 {
		req.WaitTimeoutS = seconds(timeout)
	}
	var trailer metadata.MD
	r, err := c.tasks.Submit(ctx, req, grpc.Trailer(&trailer))
	if err != nil {
		return Submitted{}, wrapWith(err, trailer)
	}
	if req.GetTask().GetOrderingKey() != "" && r.GetOutcome() == pb.SubmitOutcome_SUBMIT_OUTCOME_UNSPECIFIED {
		// A server older than ordering keys ignored the key and admitted the task.
		return submitted(r), &Error{Code: codes.Unimplemented, Message: "the server does not support Task.OrderingKey; the task was submitted without it", BatchIndex: -1}
	}
	if req.OnKeyConflict == pb.KeyConflict_KEY_CONFLICT_REPLACE_QUEUED &&
		r.GetOutcome() == pb.SubmitOutcome_SUBMIT_OUTCOME_UNSPECIFIED {
		// A server older than the option ignored it: with the key in use, this submission
		// was dropped, not stored.
		return submitted(r), &Error{Code: codes.Unimplemented, Message: "the server does not support Task.Replace", BatchIndex: -1}
	}
	return submitted(r), nil
}

// SubmitBatch submits tasks atomically; they may depend on each other's keys.
func (c *Client) SubmitBatch(ctx context.Context, tasks []Task) ([]Submitted, error) {
	req := &pb.SubmitBatchRequest{}
	for i, t := range tasks {
		if t.Replace || t.RenewPosition {
			return nil, &Error{Code: codes.InvalidArgument, Message: fmt.Sprintf("tasks[%d]: Replace and RenewPosition are not supported in a batch", i), BatchIndex: i}
		}
		// The mode is one per batch: every task names the same one.
		if t.RejectConflict != tasks[0].RejectConflict || t.ReturnExisting != tasks[0].ReturnExisting {
			return nil, &Error{Code: codes.InvalidArgument, Message: fmt.Sprintf("tasks[%d]: set RejectConflict or ReturnExisting on every task of a batch or on none", i), BatchIndex: i}
		}
		req.Tasks = append(req.Tasks, t.Spec())
	}
	if len(tasks) > 0 {
		first, err := tasks[0].request()
		if err != nil {
			return nil, err
		}
		req.OnKeyConflict = first.OnKeyConflict
	}
	var trailer metadata.MD
	r, err := c.tasks.SubmitBatch(ctx, req, grpc.Trailer(&trailer))
	if err != nil {
		return nil, wrapWith(err, trailer)
	}
	out := make([]Submitted, len(r.Tasks))
	for i, s := range r.Tasks {
		out[i] = submitted(s)
		if i < len(req.Tasks) && req.Tasks[i].GetOrderingKey() != "" && s.GetOutcome() == pb.SubmitOutcome_SUBMIT_OUTCOME_UNSPECIFIED {
			return out, &Error{Code: codes.Unimplemented, Message: "the server does not support Task.OrderingKey; the batch was submitted without it", BatchIndex: -1}
		}
		if req.OnKeyConflict == pb.KeyConflict_KEY_CONFLICT_REJECT && s.GetOutcome() == pb.SubmitOutcome_SUBMIT_OUTCOME_UNSPECIFIED {
			// A server older than the option ignored it and admitted the batch.
			return out, &Error{Code: codes.Unimplemented, Message: "the server does not support RejectConflict for batches; the batch was submitted without it", BatchIndex: -1}
		}
	}
	return out, nil
}

// Get returns a task's details.
func (c *Client) Get(ctx context.Context, ref Ref) (*TaskInfo, error) {
	r, err := c.tasks.Get(ctx, &pb.GetRequest{Task: ref.pb()})
	if err != nil {
		return nil, wrap(err)
	}
	return taskInfo(r), nil
}

// Wait waits up to timeout (0: the server's limit) for a task to finish. When it is still
// running at the timeout, the error is a [*Error] with [Error.Timeout] true.
func (c *Client) Wait(ctx context.Context, ref Ref, timeout time.Duration) (*Result, error) {
	req := &pb.WaitRequest{Task: ref.pb()}
	if timeout > 0 {
		req.TimeoutS = seconds(timeout)
	}
	r, err := c.tasks.Wait(ctx, req)
	if err != nil {
		return nil, wrap(err)
	}
	return result(r), nil
}

// Cancel cancels a task that has not finished; it reports whether it did.
func (c *Client) Cancel(ctx context.Context, ref Ref) (bool, error) {
	r, err := c.tasks.Cancel(ctx, &pb.CancelRequest{Task: ref.pb()})
	if err != nil {
		return false, wrap(err)
	}
	return r.GetCancelled(), nil
}

// Update names what to change on a task that has not finished. Zero fields keep their value.
type Update struct {
	// Priority moves the task to this class; "" keeps its class.
	Priority Priority
	// DeadlineIn sets the deadline this far from now.
	DeadlineIn time.Duration
	// DeadlineAt sets the deadline to this time.
	DeadlineAt time.Time
	// ClearDeadline removes the deadline.
	ClearDeadline bool
	// Tags, when not nil, replace the task's tags (an empty map removes them).
	Tags map[string]string
}

// Updated is the result of [Client.Update].
type Updated struct {
	ID uint64
	// Status is the task's status, which an update never changes.
	Status Status
	// Deferred reports that an attempt is running: it is not interrupted, and the new values
	// apply to what follows it.
	Deferred bool
	// Changed is false when every value given was already in place.
	Changed bool
}

// Update changes the priority, deadline or tags of a task that has not finished. A waiting
// task takes the new values at once and keeps its place by submission order in its new class.
// It fails with [NotFound] for an unknown or finished task and with [FailedPrecondition] for a
// task with a fixed start.
func (c *Client) Update(ctx context.Context, ref Ref, u Update) (Updated, error) {
	req := &pb.UpdateRequest{
		Task: ref.pb(), Priority: u.Priority.pb(), ClearDeadline: u.ClearDeadline,
		SetTags: u.Tags != nil, Tags: u.Tags,
	}
	if u.DeadlineIn > 0 {
		req.DeadlineInS = seconds(u.DeadlineIn)
	}
	if !u.DeadlineAt.IsZero() {
		ns := u.DeadlineAt.UnixNano()
		req.DeadlineUnixNs = &ns
	}
	r, err := c.tasks.Update(ctx, req)
	if err != nil {
		return Updated{}, wrap(err)
	}
	return Updated{
		ID: r.GetId(), Status: statusOf(r.GetStatus()), Deferred: r.GetDeferred(),
		Changed: r.GetChanged(),
	}, nil
}

// ListOptions select tasks for [Client.ListTasks].
type ListOptions struct {
	Pools    []string // empty: every pool the caller may see
	Statuses []Status // empty: any
	After    uint64   // only ids above this
	Limit    uint32   // 0: 100, at most the server's limits.max_list
}

// TaskPage is one page of tasks in id order.
type TaskPage struct {
	Tasks []*TaskInfo
	// NextAfter is passed as After for the next page; 0 when this page is the last.
	NextAfter uint64
	// UnavailableShards could not be reached; their tasks are missing from the page.
	UnavailableShards []string
}

// ListTasks returns one page of tasks.
func (c *Client) ListTasks(ctx context.Context, o ListOptions) (*TaskPage, error) {
	req := &pb.ListTasksRequest{Pools: o.Pools, After: o.After, Limit: o.Limit}
	for _, s := range o.Statuses {
		v, err := s.pb()
		if err != nil {
			return nil, err
		}
		req.Statuses = append(req.Statuses, v)
	}
	r, err := c.tasks.ListTasks(ctx, req)
	if err != nil {
		return nil, wrap(err)
	}
	page := &TaskPage{NextAfter: r.GetNextAfter(), UnavailableShards: r.GetUnavailableShards()}
	for _, t := range r.Tasks {
		page.Tasks = append(page.Tasks, taskInfo(t))
	}
	return page, nil
}

// WatchOptions select events for [Client.Watch].
type WatchOptions struct {
	Pools []string // empty: every pool the caller may see
	IDs   []uint64
	Keys  []string
	// AllTransitions streams every status change, not only terminal statuses.
	AllTransitions bool
}

// Event is a task's status change.
type Event struct {
	ID      uint64
	Pool    string
	Key     string
	Status  Status
	Attempt uint32
	At      time.Time
	Message string
	// Priority is the task's class at the time of the event ("" from older servers).
	Priority Priority
	// Updated reports that the event is not a status change: the task was changed by
	// [Client.Update] and Status is its unchanged status. Only delivered with AllTransitions.
	Updated bool
	// Kind says what the event reports. Kinds other than EventTransition are only delivered with
	// AllTransitions, and Status is then the task's status at that moment.
	Kind EventKind
}

// EventKind is what an [Event] reports.
type EventKind string

const (
	// EventTransition: the task changed status.
	EventTransition EventKind = "transition"
	// EventUpdated: the task was changed by [Client.Update].
	EventUpdated EventKind = "updated"
	// EventReplaced: a submission with the task's key replaced it, or was stored to run
	// after it ([Task.Replace]).
	EventReplaced EventKind = "replaced"
	// EventSuccessor: the task was created from a stored submission when the task that held
	// its key ended; Message names that task.
	EventSuccessor EventKind = "successor"
)

func eventKind(k pb.EventKind) EventKind {
	switch k {
	case pb.EventKind_EVENT_KIND_UPDATED:
		return EventUpdated
	case pb.EventKind_EVENT_KIND_REPLACED:
		return EventReplaced
	case pb.EventKind_EVENT_KIND_SUCCESSOR:
		return EventSuccessor
	default:
		return EventTransition
	}
}

// Watch streams status events until ctx ends or the stream fails. The stream ends with an
// error when the server shuts down or loses leadership; watch again to continue.
func (c *Client) Watch(ctx context.Context, o WatchOptions) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		s, err := c.tasks.Watch(ctx, &pb.WatchRequest{Pools: o.Pools, Ids: o.IDs, Keys: o.Keys, AllTransitions: o.AllTransitions})
		if err != nil {
			yield(Event{}, wrap(err))
			return
		}
		for {
			e, err := s.Recv()
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return
			}
			if err != nil {
				yield(Event{}, wrap(err))
				return
			}
			ev := Event{
				ID: e.GetId(), Pool: e.GetPool(), Key: e.GetKey(), Status: statusOf(e.GetStatus()),
				Attempt: e.GetAttempt(), At: time.Unix(0, e.GetAtUnixNs()), Message: e.GetMessage(),
				Priority: priority(e.GetPriority()),
				Updated:  e.GetKind() == pb.EventKind_EVENT_KIND_UPDATED,
				Kind:     eventKind(e.GetKind()),
			}
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// Snapshot returns the server's pools, queues and totals (metrics role).
func (c *Client) Snapshot(ctx context.Context) (*pb.SnapshotResponse, error) {
	r, err := c.admin.Snapshot(ctx, &pb.Empty{})
	return r, wrap(err)
}

// OrderingInfo is an ordering key as the server holds it.
type OrderingInfo struct {
	Pool string
	Key  string
	// Tasks of the key that have not finished.
	Tasks uint64
	// Head is the oldest of them, the only one that may run; 0 if the key has no tasks.
	Head uint64
	// Effective is the class the head runs at: the highest among the key's tasks and its
	// floor. Empty without tasks or a floor.
	Effective Priority
	// Floor is the class floor, or empty.
	Floor Priority
	// Persist says the floor stays when the key has no tasks left.
	Persist bool
	// Halted says a failed task halted the key: nothing of it runs until [Client.Resume].
	Halted bool
}

// Ordering returns an ordering key of pool (the default pool if empty): its tasks, the one
// at the front, the class that one runs at and whether the key is halted (metrics role). A
// key with no tasks and no kept floor has Tasks 0.
func (c *Client) Ordering(ctx context.Context, pool, key string) (*OrderingInfo, error) {
	r, err := c.admin.GetOrdering(ctx, &pb.OrderingRequest{Pool: pool, Key: key})
	if err != nil {
		return nil, wrap(err)
	}
	info := &OrderingInfo{
		Pool: r.GetPool(), Key: r.GetKey(), Tasks: r.GetTasks(), Head: r.GetHead(),
		Effective: priority(r.GetEffective()), Persist: r.GetPersist(), Halted: r.GetHalted(),
	}
	if r.Floor != nil {
		info.Floor = priority(r.GetFloor())
	}
	return info, nil
}

// HaltedOrderings lists the halted ordering keys of pool, in name order (metrics role). A
// limit of 0 takes the server's default.
func (c *Client) HaltedOrderings(ctx context.Context, pool string, limit uint32) ([]string, error) {
	r, err := c.admin.ListHaltedOrderings(ctx, &pb.ListHaltedOrderingsRequest{Pool: pool, Limit: limit})
	if err != nil {
		return nil, wrap(err)
	}
	return r.GetKeys(), nil
}

// SetOrderingFloor sets the class the front of an ordering key never runs below, whatever
// the classes of the key's tasks; an empty floor removes it. No task's own class changes.
// The floor goes when the key has no tasks left unless persist is set; the server limits how
// many keys keep one (ResourceExhausted beyond it). Admin role.
func (c *Client) SetOrderingFloor(ctx context.Context, pool, key string, floor Priority, persist bool) error {
	req := &pb.SetOrderingFloorRequest{Pool: pool, Key: key, Persist: persist}
	if floor != "" {
		p := floor.pb()
		if p == pb.Priority_PRIORITY_UNSPECIFIED {
			return &Error{Code: codes.InvalidArgument, Message: fmt.Sprintf("unknown priority %q", string(floor)), BatchIndex: -1}
		}
		req.Floor = &p
	}
	_, err := c.admin.SetOrderingFloor(ctx, req)
	return wrap(err)
}

// Resume resumes an ordering key that a failed task halted ([Task.HaltOnFailure]): its
// oldest task is offered again. It reports whether the key was halted; resuming one that is
// not is not an error. Admin role.
func (c *Client) Resume(ctx context.Context, pool, orderingKey string) (bool, error) {
	r, err := c.admin.Resume(ctx, &pb.ResumeRequest{Pool: pool, OrderingKey: orderingKey})
	if err != nil {
		return false, wrap(err)
	}
	return r.GetResumed(), nil
}

// Ref names a task by id or by key.
type Ref struct {
	id  uint64
	key string
	set bool
}

// ID refers to a task by id.
func ID(id uint64) Ref { return Ref{id: id, set: true} }

// Key refers to a task by key.
func Key(key string) Ref { return Ref{key: key} }

func (r Ref) pb() *pb.TaskRef {
	if r.set {
		return &pb.TaskRef{Target: &pb.TaskRef_Id{Id: r.id}}
	}
	return &pb.TaskRef{Target: &pb.TaskRef_Key{Key: r.key}}
}

func seconds(d time.Duration) *float64 {
	s := d.Seconds()
	return &s
}
