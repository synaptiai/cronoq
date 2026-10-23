package cronoq_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cronoq "github.com/synaptiai/cronoq/sdks/go"
)

func client(t *testing.T, s *server) *cronoq.Client {
	t.Helper()
	c, err := cronoq.Dial(s.target(), cronoq.Options{Token: clientToken})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// worker runs handler on pool "default" of s until the test ends; the returned function stops
// it and returns RunWorker's error.
func worker(t *testing.T, target string, opts cronoq.WorkerOptions, handler cronoq.Handler) func() error {
	t.Helper()
	opts.Target = target
	if opts.Pool == "" {
		opts.Pool = "default"
	}
	if opts.Token == "" {
		opts.Token = workerToken
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cronoq.RunWorker(ctx, opts, handler) }()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() {
			cancel()
			err = <-done
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func upper(_ context.Context, l *cronoq.Lease) ([]byte, error) {
	return bytes.ToUpper(l.Payload), nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 10s")
		}
	}
}

func ctxFor(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestSubmitWaitAndInspect(t *testing.T) {
	s := startServer(t)
	c := client(t, s)
	ctx := ctxFor(t)
	worker(t, s.target(), cronoq.WorkerOptions{Slots: 2, Name: "go"}, upper)

	sub, err := c.SubmitAndWait(ctx, cronoq.Task{Payload: []byte("hello"), Key: "greeting"}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if sub.Result == nil || !sub.Result.OK() || string(sub.Result.Output) != "HELLO" {
		t.Fatalf("result = %+v", sub.Result)
	}
	// The key makes resubmission idempotent: the finished task and its output come back.
	again, err := c.SubmitAndWait(ctx, cronoq.Task{Payload: []byte("other"), Key: "greeting"}, 10*time.Second)
	if err != nil || !again.Duplicate || again.ID != sub.ID || string(again.Result.Output) != "HELLO" {
		t.Fatalf("resubmission = %+v, %v", again, err)
	}
	info, err := c.Get(ctx, cronoq.Key("greeting"))
	if err != nil || info.ID != sub.ID || info.Status != cronoq.Succeeded || !info.Status.Done() {
		t.Fatalf("get = %+v, %v", info, err)
	}
	page, err := c.ListTasks(ctx, cronoq.ListOptions{Statuses: []cronoq.Status{cronoq.Succeeded}})
	if err != nil || len(page.Tasks) != 1 || page.Tasks[0].ID != sub.ID {
		t.Fatalf("list = %+v, %v", page, err)
	}
	_, err = c.Get(ctx, cronoq.ID(1<<50))
	var e *cronoq.Error
	if !errors.As(err, &e) || !e.NotFound() {
		t.Fatalf("unknown task: %v", err)
	}
}

func TestBatchWaitCancelAndWatch(t *testing.T) {
	s := startServer(t)
	c := client(t, s)
	ctx := ctxFor(t)

	events := make(chan cronoq.Event, 16)
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go func() {
		for ev, err := range c.Watch(watchCtx, cronoq.WatchOptions{Keys: []string{"first", "second"}}) {
			if err != nil {
				return
			}
			events <- ev
		}
	}()
	time.Sleep(200 * time.Millisecond)

	subs, err := c.SubmitBatch(ctx, []cronoq.Task{
		{Payload: []byte("a"), Key: "first"},
		{Payload: []byte("b"), Key: "second", After: []cronoq.Dep{{Ref: cronoq.Key("first")}}},
	})
	if err != nil || len(subs) != 2 {
		t.Fatalf("batch = %v, %v", subs, err)
	}
	// Nothing serves the pool yet: waiting times out and the task stays queued.
	_, err = c.Wait(ctx, cronoq.ID(subs[0].ID), 200*time.Millisecond)
	var e *cronoq.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Fatalf("wait: %v", err)
	}
	if ok, err := c.Cancel(ctx, cronoq.Key("first")); err != nil || !ok {
		t.Fatalf("cancel = %v, %v", ok, err)
	}
	res, err := c.Wait(ctx, cronoq.ID(subs[1].ID), 10*time.Second)
	if err != nil || res.Status == cronoq.Succeeded {
		t.Fatalf("dependent of a cancelled task: %+v, %v", res, err)
	}
	seen := map[string]cronoq.Status{}
	for len(seen) < 2 {
		select {
		case ev := <-events:
			seen[ev.Key] = ev.Status
		case <-time.After(10 * time.Second):
			t.Fatalf("events = %v", seen)
		}
	}
	if seen["first"] != cronoq.Cancelled || !seen["second"].Done() {
		t.Fatalf("events = %v", seen)
	}
}

func TestHandlerOutcomes(t *testing.T) {
	s := startServer(t)
	c := client(t, s)
	ctx := ctxFor(t)
	var mu sync.Mutex
	attempts := map[uint64][]uint32{}
	worker(t, s.target(), cronoq.WorkerOptions{Slots: 4}, func(ctx context.Context, l *cronoq.Lease) ([]byte, error) {
		mu.Lock()
		attempts[l.TaskID] = append(attempts[l.TaskID], l.Attempt)
		mu.Unlock()
		switch string(l.Payload) {
		case "permanent":
			return nil, cronoq.Permanent(errors.New("bad input"))
		case "flaky":
			if l.Attempt == 1 {
				return nil, cronoq.RetryAfter(errors.New("busy"), 50*time.Millisecond)
			}
			return []byte(fmt.Sprintf("attempt %d", l.Attempt)), nil
		case "cancel":
			return nil, cronoq.ErrCancelTask
		case "panic":
			panic("boom")
		}
		return nil, errors.New("unexpected")
	})
	want := map[string]cronoq.Status{
		"permanent": cronoq.Failed, "flaky": cronoq.Succeeded, "cancel": cronoq.Cancelled,
		"panic": cronoq.DeadLettered,
	}
	for payload, st := range want {
		sub, err := c.SubmitAndWait(ctx, cronoq.Task{Payload: []byte(payload)}, 20*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if sub.Result.Status != st {
			t.Errorf("%s: status %s (%s), want %s", payload, sub.Result.Status, sub.Result.Message, st)
		}
		if payload == "flaky" {
			// (task id, attempt) is a fencing token: the retry has a higher attempt number.
			mu.Lock()
			got := attempts[sub.ID]
			mu.Unlock()
			if string(sub.Result.Output) != "attempt 2" || len(got) != 2 || got[0] != 1 || got[1] != 2 {
				t.Errorf("flaky: output %q, attempts %v", sub.Result.Output, got)
			}
		}
	}
}

func TestAbortCancelsTheHandler(t *testing.T) {
	s := startServer(t)
	c := client(t, s)
	ctx := ctxFor(t)
	var aborted atomic.Int32
	worker(t, s.target(), cronoq.WorkerOptions{}, func(ctx context.Context, l *cronoq.Lease) ([]byte, error) {
		select {
		case <-ctx.Done():
			aborted.Add(1)
			return nil, ctx.Err()
		case <-time.After(20 * time.Second):
			return []byte("late"), nil
		}
	})
	sub, err := c.SubmitAndWait(ctx, cronoq.Task{Timeout: 200 * time.Millisecond, MaxAttempts: 1}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if sub.Result.Status == cronoq.Succeeded {
		t.Fatalf("result = %+v", sub.Result)
	}
	deadline := time.Now().Add(5 * time.Second)
	for aborted.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if aborted.Load() != 1 {
		t.Fatalf("handler contexts cancelled: %d", aborted.Load())
	}
}

func TestHeartbeatsKeepLongLeases(t *testing.T) {
	s := startServer(t) // lease_ttl_s = 1
	c := client(t, s)
	worker(t, s.target(), cronoq.WorkerOptions{}, func(ctx context.Context, l *cronoq.Lease) ([]byte, error) {
		time.Sleep(2500 * time.Millisecond)
		return []byte(fmt.Sprint(l.Attempt)), nil
	})
	sub, err := c.SubmitAndWait(ctxFor(t), cronoq.Task{}, 20*time.Second)
	if err != nil || !sub.Result.OK() || string(sub.Result.Output) != "1" {
		t.Fatalf("result = %+v, %v", sub.Result, err)
	}
}

func TestDrainFinishesRunningLeases(t *testing.T) {
	s := startServer(t)
	c := client(t, s)
	ctx := ctxFor(t)
	started := make(chan struct{})
	stop := worker(t, s.target(), cronoq.WorkerOptions{}, func(ctx context.Context, l *cronoq.Lease) ([]byte, error) {
		close(started)
		time.Sleep(500 * time.Millisecond)
		return []byte("done"), ctx.Err()
	})
	sub, err := c.Submit(ctx, cronoq.Task{})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := stop(); err != nil {
		t.Fatalf("RunWorker: %v", err)
	}
	res, err := c.Wait(ctx, cronoq.ID(sub.ID), 5*time.Second)
	if err != nil || !res.OK() || string(res.Output) != "done" {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

func TestWorkerReconnectsAfterRestart(t *testing.T) {
	s := startServer(t)
	worker(t, s.target(), cronoq.WorkerOptions{InitialBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}, upper)
	admin, err := cronoq.Dial(s.target(), cronoq.Options{Token: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	connected := func() bool {
		snap, err := admin.Snapshot(ctxFor(t))
		return err == nil && snap.GetWorkersConnected() == 1
	}
	waitFor(t, connected)
	// The stream drops with the server; the worker comes back to the restarted one.
	s.stop()
	s.start()
	waitFor(t, connected)
	c := client(t, s)
	sub, err := c.SubmitAndWait(ctxFor(t), cronoq.Task{Payload: []byte("back")}, 20*time.Second)
	if err != nil || string(sub.Result.Output) != "BACK" {
		t.Fatalf("result = %+v, %v", sub.Result, err)
	}
}

func TestRejectedWorkersStop(t *testing.T) {
	s := startServer(t)
	stop := make(chan error, 1)
	go func() {
		stop <- cronoq.RunWorker(context.Background(), cronoq.WorkerOptions{Target: s.target(), Pool: "default", Token: "wrong-token-0123456789"}, upper)
	}()
	select {
	case err := <-stop:
		if err == nil {
			t.Fatal("RunWorker accepted a wrong token")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RunWorker kept retrying a wrong token")
	}
	err := cronoq.RunWorker(context.Background(), cronoq.WorkerOptions{Target: s.target(), Pool: "nope", Token: workerToken}, upper)
	var e *cronoq.Error
	if !errors.As(err, &e) || !e.NotFound() {
		t.Fatalf("unknown pool: %v", err)
	}
}

func TestWorkerTLSNeedsHTTPS(t *testing.T) {
	// Nothing listens there: the settings are refused before any connection is tried.
	err := cronoq.RunWorker(ctxFor(t), cronoq.WorkerOptions{
		Target: "http://127.0.0.1:1", Pool: "default", TLS: &tls.Config{}, NoReconnect: true,
	}, upper)
	if err == nil || !strings.Contains(err.Error(), "need an https:// target") {
		t.Fatalf("RunWorker: %v", err)
	}
}

func TestWorkersFollowTheirPoolToItsShard(t *testing.T) {
	pools := "\n[[pools]]\nname = \"default\"\nmax = 4\n[[pools]]\nname = \"llm\"\nmax = 4\n[[pools]]\nname = \"spare\"\nmax = 1\n"
	shards := func(cpu, gpu *server, cpuPools, gpuPools string) string {
		return fmt.Sprintf("%s\n[[shards]]\nname = \"cpu\"\nnumber = 1\npools = %s\nendpoint = %q\n[[shards]]\nname = \"gpu\"\nnumber = 2\npools = %s\nendpoint = %q\n",
			pools, cpuPools, cpu.target(), gpuPools, gpu.target())
	}
	cpu := newServer(t, `shard = "cpu"`, "")
	gpu := newServer(t, `shard = "gpu"`, "")
	cpu.tables = shards(cpu, gpu, `["default", "spare"]`, `["llm"]`)
	gpu.tables = cpu.tables
	cpu.start()
	gpu.start()
	// Pointed at cpu, a worker for pool llm registers with gpu and runs its tasks.
	worker(t, cpu.target(), cronoq.WorkerOptions{Pool: "llm"}, upper)
	sub, err := client(t, cpu).SubmitAndWait(ctxFor(t), cronoq.Task{Pool: "llm", Payload: []byte("gpu")}, 20*time.Second)
	if err != nil || string(sub.Result.Output) != "GPU" {
		t.Fatalf("result = %+v, %v", sub.Result, err)
	}
	cpu.stop()
	gpu.stop()

	// Shard maps that each name the other server send the worker back and forth until it stops.
	gpu.tables = shards(cpu, gpu, `["default", "llm"]`, `["spare"]`)
	cpu.start()
	gpu.start()
	err = cronoq.RunWorker(ctxFor(t), cronoq.WorkerOptions{Target: cpu.target(), Pool: "llm", Token: workerToken}, upper)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("redirected more than 3 times")) {
		t.Fatalf("redirect loop: %v", err)
	}
}
