package cronoq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/synaptiai/cronoq/sdks/go/cronoqpb"
)

// MaxRedirects is how many shard redirects in a row a worker follows before it gives up.
const MaxRedirects = 3

// endpointHeader names the endpoint of the shard that serves a refused pool.
const endpointHeader = "cronoq-shard-endpoint"

// WorkerOptions configure [RunWorker].
type WorkerOptions struct {
	// Target is the server: "http://host:port", "https://host:port" or "host:port".
	Target string
	// Pool to serve.
	Pool string
	// Slots is how many leases run at once (default 1; the server may grant fewer).
	Slots int
	// Token is the bearer token (role worker).
	Token string
	// Name is shown in server logs and worker listings.
	Name string
	// Labels of this worker: tasks that require labels are leased only to workers that have
	// all of them.
	Labels map[string]string
	// TLS configures https:// targets (nil: the system roots). A ServerName set here applies
	// to Target only, not to shards the worker is redirected to.
	TLS *tls.Config
	// NoReconnect makes RunWorker return when the connection fails instead of reconnecting.
	NoReconnect bool
	// InitialBackoff and MaxBackoff bound the reconnect delay (defaults 200ms and 10s).
	InitialBackoff, MaxBackoff time.Duration
	// DrainTimeout is how long running handlers may finish after the context ends (default
	// 30s); the rest are cancelled.
	DrainTimeout time.Duration
	// MaxMessageBytes caps lease messages from the server (default 64 MiB).
	MaxMessageBytes int
	// InsecureRedirectHTTP lets the worker follow a shard redirect to plain http:// on
	// another host with its token. Without it, redirects keep TLS and use plain http only to
	// loopback. For trusted networks only.
	InsecureRedirectHTTP bool
	// Logger receives the worker's log messages (default slog.Default()).
	Logger *slog.Logger
	// DialOptions are added to the gRPC dial options.
	DialOptions []grpc.DialOption
}

// Lease is one attempt at a task, given to a [Handler].
//
// (TaskID, Attempt) is a fencing token: attempt numbers never repeat for a task, also across
// failovers, so record the pair with the handler's side effect and let only the newest
// attempt write.
type Lease struct {
	// ID identifies the lease; it carries the leader's epoch, so it never repeats.
	ID          uint64
	TaskID      uint64
	Attempt     uint32
	Payload     []byte
	ContentType string
	Kind        string
	Key         string
	Pool        string
	Priority    Priority
	Tags        map[string]string
	// Timeout of this attempt (0: none).
	Timeout time.Duration
	// Deadline of the task (zero: none).
	Deadline time.Time
}

// Handler runs one lease and returns the task's output. The context ends when the server
// aborts the attempt (timeout, cancellation, lease loss) or the drain timeout passes; the
// outcome is then not reported. Errors are reported as retryable failures, except:
//   - [Permanent] errors fail the task without retrying;
//   - [RetryAfter] errors retry no earlier than the given delay;
//   - [Reject] hands the lease back unrun, to another worker if one has a free slot;
//   - [ErrCancelTask] ends the task as cancelled.
//
// A panicking handler fails its attempt as retryable.
type Handler func(ctx context.Context, lease *Lease) ([]byte, error)

// ErrCancelTask, returned by a handler, ends the task as cancelled.
var ErrCancelTask = errors.New("cronoq: task cancelled by its handler")

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks a handler error as permanent: the task fails without retrying.
func Permanent(err error) error { return permanentError{err} }

type retryAfterError struct {
	err   error
	after time.Duration
}

func (e retryAfterError) Error() string { return e.err.Error() }
func (e retryAfterError) Unwrap() error { return e.err }

// RetryAfter marks a handler error as retryable no earlier than after.
func RetryAfter(err error, after time.Duration) error { return retryAfterError{err, after} }

type rejectError struct{ reason string }

func (e rejectError) Error() string { return "cronoq: lease rejected: " + e.reason }

// Reject hands the lease back without running it.
func Reject(reason string) error { return rejectError{reason} }

// RunWorker connects to the server, registers for the pool and runs handler for each lease,
// at most Slots at a time, until ctx ends; it then drains running handlers for up to
// DrainTimeout and returns nil. It reconnects with backoff when the connection drops or the
// server steps down, and follows the server to the shard that serves the pool. It returns an
// error for failures that retrying cannot fix (bad credentials, unknown pool, ...), or for any
// failure with NoReconnect.
func RunWorker(ctx context.Context, opts WorkerOptions, handler Handler) error {
	if opts.Pool == "" {
		return errors.New("cronoq: WorkerOptions.Pool is required")
	}
	if opts.Slots <= 0 {
		opts.Slots = 1
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = 200 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 10 * time.Second
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = 30 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if _, secure, err := endpoint(opts.Target); err != nil {
		return err
	} else if !secure && opts.TLS != nil {
		// Connecting in plaintext would silently drop the TLS settings.
		return fmt.Errorf("cronoq: worker TLS settings need an https:// target, not %q", opts.Target)
	}
	w := &worker{opts: opts, handler: handler, log: opts.Logger}
	backoff := opts.InitialBackoff
	current := opts.Target
	redirects := 0
	for {
		end := w.session(ctx, current, &backoff)
		if end.shutdown {
			return nil
		}
		if end.err != nil {
			to, err := redirect(&opts, current, end.err)
			if err != nil {
				return err
			}
			if to != "" {
				redirects++
				if redirects > MaxRedirects {
					return fmt.Errorf("cronoq: worker redirected more than %d times in a row: %w", MaxRedirects, wrap(end.err))
				}
				w.log.Info("cronoq: pool is served by another shard; connecting there", "pool", opts.Pool, "from", current, "to", to)
				current = to
				continue
			}
		}
		redirects = 0
		if end.connectFailed && current != opts.Target {
			// The shard is unreachable: ask the configured endpoint where the pool is.
			w.log.Warn("cronoq: shard unreachable; returning to the configured endpoint", "endpoint", current)
			current = opts.Target
		}
		if end.err != nil && fatal(status.Code(end.err)) {
			return fmt.Errorf("cronoq: worker rejected: %w", wrap(end.err))
		}
		if opts.NoReconnect {
			if end.err != nil {
				return fmt.Errorf("cronoq: worker stream failed: %w", wrap(end.err))
			}
			return fmt.Errorf("cronoq: worker stream ended: %s", end.reason)
		}
		delay := backoff
		if end.now {
			delay = 0
		}
		if end.err != nil {
			w.log.Warn("cronoq: worker connection failed; reconnecting", "error", end.err, "retry_in", delay)
		} else {
			w.log.Info("cronoq: worker stream ended; reconnecting", "reason", end.reason, "retry_in", delay)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		backoff = min(backoff*2, opts.MaxBackoff)
	}
}

// fatal reports errors that retrying cannot fix.
func fatal(c codes.Code) bool {
	switch c {
	case codes.Unauthenticated, codes.PermissionDenied, codes.NotFound, codes.InvalidArgument,
		codes.FailedPrecondition, codes.Unimplemented:
		return true
	}
	return false
}

// redirect returns the endpoint a refusal sends the worker to, "" when it is not a redirect,
// or an error when the endpoint may not be followed from current.
func redirect(opts *WorkerOptions, current string, err error) (string, error) {
	s, ok := status.FromError(err)
	if !ok || s.Code() != codes.FailedPrecondition {
		return "", nil
	}
	var target string
	var se interface{ trailer() metadata.MD }
	if errors.As(err, &se) {
		if v := se.trailer().Get(endpointHeader); len(v) > 0 {
			target = v[0]
		}
	}
	if target == "" {
		return "", nil
	}
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		return "", fmt.Errorf("cronoq: the server redirected the worker to %q, which is not an http(s) endpoint", target)
	}
	if _, _, err := endpoint(target); err != nil {
		return "", fmt.Errorf("cronoq: the server redirected the worker to %q, which is not a host and port", target)
	}
	plain := strings.HasPrefix(target, "http://")
	if plain && strings.HasPrefix(current, "https://") {
		return "", fmt.Errorf("cronoq: the server redirected the worker from https to plain http (%q); refusing", target)
	}
	if plain && opts.Token != "" && !isLoopback(target) {
		if !opts.InsecureRedirectHTTP {
			return "", fmt.Errorf("cronoq: the server redirected the worker to %q, which would send its token over plain http to another host; refusing (InsecureRedirectHTTP allows it)", target)
		}
		opts.Logger.Warn("cronoq: following a redirect that sends the worker token over plain http", "endpoint", target)
	}
	return target, nil
}

// trailerError carries a stream's trailer metadata with its status.
type trailerError struct {
	err error
	md  metadata.MD
}

func (e trailerError) Error() string              { return e.err.Error() }
func (e trailerError) Unwrap() error              { return e.err }
func (e trailerError) GRPCStatus() *status.Status { s, _ := status.FromError(e.err); return s }
func (e trailerError) trailer() metadata.MD       { return e.md }

type worker struct {
	opts    WorkerOptions
	handler Handler
	log     *slog.Logger
}

type sessionEnd struct {
	shutdown      bool   // ctx ended and running handlers were drained
	err           error  // the stream failed
	reason        string // the stream ended without an error
	connectFailed bool   // no connection could be made
	now           bool   // reconnect without waiting (leadership moved)
}

type running struct {
	cancel  context.CancelFunc
	aborted bool
}

func (w *worker) session(ctx context.Context, target string, backoff *time.Duration) sessionEnd {
	opts := w.opts
	dopts := Options{Token: opts.Token, TLS: opts.TLS, MaxMessageBytes: opts.MaxMessageBytes, DialOptions: opts.DialOptions, Logger: w.log}
	if target != opts.Target && opts.TLS != nil && opts.TLS.ServerName != "" {
		// A fixed server name belongs to the configured endpoint, not to the shard's.
		cfg := opts.TLS.Clone()
		cfg.ServerName = ""
		dopts.TLS = cfg
	}
	conn, err := dial(target, dopts)
	if err != nil {
		return sessionEnd{err: status.Error(codes.InvalidArgument, err.Error())}
	}
	defer conn.Close()
	streamCtx, cancelStream := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelStream()
	stream, err := pb.NewWorkersClient(conn).Work(streamCtx)
	if err != nil {
		return sessionEnd{err: err, connectFailed: status.Code(err) == codes.Unavailable}
	}
	var sendMu sync.Mutex
	send := func(m *pb.WorkerMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(m)
	}
	labels := opts.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	if err := send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Register{Register: &pb.Register{
		Pool: opts.Pool, Slots: uint32(opts.Slots), WorkerName: opts.Name,
		ProtocolVersion: ProtocolVersion, Labels: labels,
	}}}); err != nil && !errors.Is(err, io.EOF) {
		return sessionEnd{err: err, connectFailed: status.Code(err) == codes.Unavailable}
	}
	first, err := stream.Recv()
	if err != nil {
		return sessionEnd{err: trailerError{err, stream.Trailer()}, connectFailed: status.Code(err) == codes.Unavailable}
	}
	if sd := first.GetShutdown(); sd != nil {
		return sessionEnd{reason: "server shutting down: " + sd.GetReason(), now: sd.GetNotLeader()}
	}
	reg := first.GetRegistered()
	if reg == nil {
		return sessionEnd{err: status.Errorf(codes.Internal, "unexpected first server message: %v", first)}
	}
	*backoff = opts.InitialBackoff
	ttl := time.Duration(reg.GetLeaseTtlS() * float64(time.Second))
	slots := int(reg.GetSlots())
	maxOutput := int(reg.GetMaxMessageBytes()) - 1024
	w.log.Info("cronoq: worker registered", "worker", reg.GetWorkerId(), "pool", opts.Pool, "slots", slots)

	var mu sync.Mutex
	leases := map[uint64]*running{}
	var wg sync.WaitGroup
	idle := make(chan struct{}, 1)
	handlerBase, cancelHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer func() {
		cancelHandlers()
		wg.Wait()
	}()

	// When ctx ends: take no more leases, let running ones finish, then half-close.
	drained := make(chan struct{})
	draining := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-streamCtx.Done():
			return
		}
		close(draining)
		_ = send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_SetSlots{SetSlots: &pb.SetSlots{Slots: 0}}})
		deadline := time.After(opts.DrainTimeout)
		for {
			mu.Lock()
			n := len(leases)
			mu.Unlock()
			if n == 0 {
				break
			}
			select {
			case <-idle:
				continue
			case <-deadline:
			case <-streamCtx.Done():
				return
			}
			break
		}
		// Handlers still running report a retryable failure as they end.
		cancelHandlers()
		wg.Wait()
		sendMu.Lock()
		_ = stream.CloseSend()
		sendMu.Unlock()
		close(drained)
		// The server ends the stream once it has every outcome; do not wait forever.
		select {
		case <-time.After(5 * time.Second):
			cancelStream()
		case <-streamCtx.Done():
		}
	}()
	isDraining := func() bool {
		select {
		case <-draining:
			return true
		default:
			return false
		}
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if isDraining() {
				select {
				case <-drained:
				case <-time.After(opts.DrainTimeout + 5*time.Second):
				}
				return sessionEnd{shutdown: true}
			}
			if errors.Is(err, io.EOF) {
				return sessionEnd{reason: "server closed the stream"}
			}
			return sessionEnd{err: trailerError{err, stream.Trailer()}}
		}
		switch m := msg.Msg.(type) {
		case *pb.ServerMessage_Lease:
			l := m.Lease
			mu.Lock()
			busy := len(leases)
			mu.Unlock()
			if isDraining() || busy >= slots {
				reason := "no free slot"
				if isDraining() {
					reason = "worker is draining"
				}
				_ = send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Nack{Nack: &pb.Nack{Lease: l.GetLease(), Reason: reason}}})
				continue
			}
			hctx, cancel := context.WithCancel(handlerBase)
			r := &running{cancel: cancel}
			mu.Lock()
			leases[l.GetLease()] = r
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					mu.Lock()
					delete(leases, l.GetLease())
					mu.Unlock()
					cancel()
					select {
					case idle <- struct{}{}:
					default:
					}
				}()
				out := w.run(hctx, l, ttl, maxOutput, send)
				mu.Lock()
				aborted := r.aborted
				mu.Unlock()
				if !aborted && out != nil {
					_ = send(out)
				}
			}()
		case *pb.ServerMessage_Abort:
			mu.Lock()
			if r, ok := leases[m.Abort.GetLease()]; ok {
				r.aborted = true
				r.cancel()
			}
			mu.Unlock()
		case *pb.ServerMessage_Shutdown:
			if isDraining() {
				continue
			}
			return sessionEnd{reason: "server shutting down: " + m.Shutdown.GetReason(), now: m.Shutdown.GetNotLeader()}
		}
	}
}

// run runs the handler for one lease, heartbeating it, and returns the outcome to report.
func (w *worker) run(ctx context.Context, l *pb.Lease, ttl time.Duration, maxOutput int, send func(*pb.WorkerMessage) error) *pb.WorkerMessage {
	id := l.GetLease()
	if l.LeaseTtlS != nil {
		ttl = time.Duration(l.GetLeaseTtlS() * float64(time.Second))
	}
	if ttl > 0 {
		beat := time.NewTicker(max(ttl/3, 10*time.Millisecond))
		defer beat.Stop()
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-beat.C:
					_ = send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Heartbeat{Heartbeat: &pb.Heartbeat{Lease: id}}})
				case <-done:
					return
				}
			}
		}()
	}
	lease := &Lease{
		ID: id, TaskID: l.GetTaskId(), Attempt: l.GetAttempt(), Payload: l.GetPayload(),
		ContentType: l.GetContentType(), Kind: l.GetKind(), Key: l.GetKey(), Pool: l.GetPool(),
		Priority: priority(l.GetPriority()), Tags: l.GetTags(),
	}
	if l.TimeoutS != nil {
		lease.Timeout = time.Duration(l.GetTimeoutS() * float64(time.Second))
	}
	if l.DeadlineUnixNs != nil {
		lease.Deadline = time.Unix(0, l.GetDeadlineUnixNs())
	}
	out, err := w.call(ctx, lease)
	fail := func(msg string, permanent bool, after *float64) *pb.WorkerMessage {
		return &pb.WorkerMessage{Msg: &pb.WorkerMessage_Fail{Fail: &pb.Fail{Lease: id, Message: msg, Permanent: permanent, RetryAfterS: after}}}
	}
	var perm permanentError
	var later retryAfterError
	var rej rejectError
	switch {
	case err == nil && maxOutput > 0 && len(out) > maxOutput:
		return fail(fmt.Sprintf("output of %d bytes exceeds the server's message limit", len(out)), true, nil)
	case err == nil:
		return &pb.WorkerMessage{Msg: &pb.WorkerMessage_Complete{Complete: &pb.Complete{Lease: id, Output: out}}}
	case errors.Is(err, ErrCancelTask):
		return &pb.WorkerMessage{Msg: &pb.WorkerMessage_Cancelled{Cancelled: &pb.Cancelled{Lease: id}}}
	case errors.As(err, &rej):
		return &pb.WorkerMessage{Msg: &pb.WorkerMessage_Nack{Nack: &pb.Nack{Lease: id, Reason: rej.reason}}}
	case errors.As(err, &perm):
		return fail(err.Error(), true, nil)
	case errors.As(err, &later):
		return fail(err.Error(), false, seconds(later.after))
	default:
		return fail(err.Error(), false, nil)
	}
}

// call runs the handler, turning a panic into a retryable error.
func (w *worker) call(ctx context.Context, lease *Lease) (out []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			w.log.Error("cronoq: handler panicked", "task", lease.TaskID, "panic", p)
			out, err = nil, fmt.Errorf("handler panicked: %v", p)
		}
	}()
	return w.handler(ctx, lease)
}
