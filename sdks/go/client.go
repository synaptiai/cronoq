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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

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
}

func submitted(r *pb.SubmitResponse) Submitted {
	s := Submitted{ID: r.GetId(), Duplicate: r.GetDuplicate()}
	if r.Result != nil {
		s.Result = result(r.Result)
	}
	return s
}

// Submit submits one task.
func (c *Client) Submit(ctx context.Context, t Task) (Submitted, error) {
	return c.SubmitSpec(ctx, t.Spec(), false, 0)
}

// SubmitAndWait submits one task and waits up to timeout (0: the server's limit) for it to
// finish. When it is still running at the timeout, the error is a [*Error] with
// [Error.Timeout] true; the task stays submitted.
func (c *Client) SubmitAndWait(ctx context.Context, t Task, timeout time.Duration) (Submitted, error) {
	return c.SubmitSpec(ctx, t.Spec(), true, timeout)
}

// SubmitSpec submits a task described by a raw spec.
func (c *Client) SubmitSpec(ctx context.Context, spec *pb.TaskSpec, wait bool, timeout time.Duration) (Submitted, error) {
	req := &pb.SubmitRequest{Task: spec, Wait: wait}
	if wait && timeout > 0 {
		req.WaitTimeoutS = seconds(timeout)
	}
	r, err := c.tasks.Submit(ctx, req)
	if err != nil {
		return Submitted{}, wrap(err)
	}
	return submitted(r), nil
}

// SubmitBatch submits tasks atomically; they may depend on each other's keys.
func (c *Client) SubmitBatch(ctx context.Context, tasks []Task) ([]Submitted, error) {
	req := &pb.SubmitBatchRequest{}
	for _, t := range tasks {
		req.Tasks = append(req.Tasks, t.Spec())
	}
	r, err := c.tasks.SubmitBatch(ctx, req)
	if err != nil {
		return nil, wrap(err)
	}
	out := make([]Submitted, len(r.Tasks))
	for i, s := range r.Tasks {
		out[i] = submitted(s)
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
