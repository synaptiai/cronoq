package embedded

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func open(t *testing.T, cfg Config) *Scheduler {
	t.Helper()
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func submit(t *testing.T, s *Scheduler, payload string, opts *Task) uint64 {
	t.Helper()
	r, err := s.Submit([]byte(payload), opts)
	if err != nil {
		t.Fatalf("submit %q: %v", payload, err)
	}
	return r.ID
}

func next(t *testing.T, s *Scheduler) *Lease {
	t.Helper()
	l, err := s.Next("", 5*time.Second)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	return l
}

// drain takes and completes leases until the queue is empty; their payloads in order.
func drain(t *testing.T, s *Scheduler) []string {
	t.Helper()
	var out []string
	for {
		l, err := s.Next("", 100*time.Millisecond)
		if errors.Is(err, ErrTimeout) {
			return out
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if err := l.Complete(); err != nil {
			t.Fatalf("complete: %v", err)
		}
		out = append(out, string(l.Payload))
	}
}

func equal(t *testing.T, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// eventually waits for the scheduler to have applied what was reported.
func eventually(t *testing.T, s *Scheduler, ok func(*Snapshot) bool) *Snapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if ok(snap) {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("not reached: %+v", snap)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestVersion(t *testing.T) {
	v, err := Version()
	if err != nil || v == "" {
		t.Fatalf("version %q: %v", v, err)
	}
}

func TestPriorityOrder(t *testing.T) {
	s := open(t, Config{})
	submit(t, s, "low", &Task{Priority: Low})
	submit(t, s, "normal", nil)
	submit(t, s, "critical", &Task{Priority: Critical})
	submit(t, s, "high", &Task{Priority: High})
	equal(t, drain(t, s), []string{"critical", "high", "normal", "low"})
	snap := eventually(t, s, func(v *Snapshot) bool { return v.Totals["succeeded"] == 4 })
	if snap.Pools[0].Name != "default" {
		t.Fatalf("pool %q", snap.Pools[0].Name)
	}
}

func TestLeaseCarriesTheTask(t *testing.T) {
	s := open(t, Config{})
	id := submit(t, s, "hello", &Task{Key: "k1", Kind: "send", Tags: map[string]string{"a": "b"}, TimeoutMs: 60000, DeadlineMs: 30000})
	l := next(t, s)
	if l.Task != id || l.Attempt != 1 || string(l.Payload) != "hello" || l.Key != "k1" || l.Kind != "send" ||
		l.Pool != "default" || l.Tags["a"] != "b" || l.Timeout != time.Minute || l.Deadline.IsZero() {
		t.Fatalf("lease %+v", l)
	}
	info, err := s.Task(TaskRef{Key: "k1"})
	if err != nil || info.ID != id || info.Status != "running" || info.Priority != Normal {
		t.Fatalf("task %+v: %v", info, err)
	}
	if err := l.Heartbeat(); err != nil {
		t.Fatal(err)
	}
	if err := l.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := l.Complete(); !errors.Is(err, ErrLeaseGone) {
		t.Fatalf("second complete: %v", err)
	}
	// An empty payload.
	submit(t, s, "", nil)
	if l := next(t, s); len(l.Payload) != 0 || l.Key != "" {
		t.Fatalf("lease %+v", l)
	} else if err := l.Complete(); err != nil {
		t.Fatal(err)
	}
}

func TestOrderingKeys(t *testing.T) {
	s := open(t, Config{})
	submit(t, s, "a1", &Task{OrderingKey: "a", Priority: Low})
	submit(t, s, "a2", &Task{OrderingKey: "a", Priority: Low})
	submit(t, s, "n", nil)
	if err := s.SetOrderingFloor("", "a", High, false); err != nil {
		t.Fatal(err)
	}
	equal(t, drain(t, s), []string{"a1", "a2", "n"})
	if err := s.SetOrderingFloor("", "a", "urgent", false); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("bad floor: %v", err)
	}
}

func TestReplaceByKey(t *testing.T) {
	s := open(t, Config{})
	first := submit(t, s, "v1", &Task{Key: "title"})
	r, err := s.Submit([]byte("v2"), &Task{Key: "title", OnKeyConflict: "replace_queued"})
	if err != nil || r.ID != first || r.Outcome != Replaced {
		t.Fatalf("replace %+v: %v", r, err)
	}
	r, err = s.Submit([]byte("v3"), &Task{Key: "title"})
	if err != nil || r.ID != first || r.Outcome != Existing {
		t.Fatalf("existing %+v: %v", r, err)
	}
	submit(t, s, "f1", &Task{Key: "f", Fingerprint: "0a"})
	_, err = s.Submit([]byte("f2"), &Task{Key: "f", Fingerprint: "0b", OnKeyConflict: "reject"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	equal(t, drain(t, s), []string{"v2", "f1"})
}

func TestPauseAndResume(t *testing.T) {
	s := open(t, Config{})
	submit(t, s, "x", nil)
	changed, err := s.Pause(PauseTarget{}, "relay deploy")
	if err != nil || !changed {
		t.Fatalf("pause %v: %v", changed, err)
	}
	_, err = s.Next("", 50*time.Millisecond)
	var paused *PausedError
	if !errors.As(err, &paused) || paused.Reason != "relay deploy" || !errors.Is(err, ErrPaused) {
		t.Fatalf("next while paused: %v", err)
	}
	snap, _ := s.Snapshot()
	if snap.Pools[0].Paused == nil || *snap.Pools[0].Paused != "relay deploy" || snap.Pools[0].PausedTasks != 1 {
		t.Fatalf("snapshot %+v", snap.Pools[0])
	}
	if changed, err := s.Resume(PauseTarget{}); err != nil || !changed {
		t.Fatalf("resume %v: %v", changed, err)
	}
	equal(t, drain(t, s), []string{"x"})

	submit(t, s, "low", &Task{Priority: Low})
	submit(t, s, "keyed", &Task{OrderingKey: "k"})
	submit(t, s, "free", nil)
	if _, err := s.Pause(PauseTarget{Class: Low}, "backfill off"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pause(PauseTarget{OrderingKey: "k"}, "repair"); err != nil {
		t.Fatal(err)
	}
	equal(t, drain(t, s), []string{"free"})
	snap, _ = s.Snapshot()
	if snap.Pools[0].PausedClasses[Low] != "backfill off" || snap.Pools[0].Ordering.Paused != 1 {
		t.Fatalf("snapshot %+v", snap.Pools[0])
	}
	s.Resume(PauseTarget{Class: Low})
	s.Resume(PauseTarget{OrderingKey: "k"})
	equal(t, drain(t, s), []string{"keyed", "low"})
	if _, err := s.Pause(PauseTarget{}, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty reason: %v", err)
	}
}

func TestReleaseDoesNotUseARetry(t *testing.T) {
	s := open(t, Config{Pools: []Pool{{Name: "default", Retry: &Retry{MaxAttempts: 1}}}})
	id := submit(t, s, "x", nil)
	if err := next(t, s).Release(0); err != nil {
		t.Fatal(err)
	}
	l := next(t, s)
	if l.Task != id || l.Attempt != 2 {
		t.Fatalf("lease %+v", l)
	}
	if err := l.Complete(); err != nil {
		t.Fatal(err)
	}
	snap := eventually(t, s, func(v *Snapshot) bool { return v.Totals["succeeded"] == 1 })
	if snap.Totals["releases"] != 1 || snap.Totals["retries"] != 0 {
		t.Fatalf("totals %v", snap.Totals)
	}
}

func TestFailRetriesOrEnds(t *testing.T) {
	no := false
	s := open(t, Config{Pools: []Pool{{Name: "default", Retry: &Retry{MaxAttempts: 2, InitialBackoffMs: 1, Jitter: &no}}}})
	id := submit(t, s, "x", nil)
	if err := next(t, s).Fail("relay said no", true, 0); err != nil {
		t.Fatal(err)
	}
	l := next(t, s)
	if l.Task != id || l.Attempt != 2 {
		t.Fatalf("lease %+v", l)
	}
	if err := l.Fail("again", true, 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	eventually(t, s, func(v *Snapshot) bool { return v.Totals["dead_lettered"] == 1 })
	submit(t, s, "y", nil)
	if err := next(t, s).Fail("", false, 0); err != nil {
		t.Fatal(err)
	}
	eventually(t, s, func(v *Snapshot) bool { return v.Totals["failed"] == 1 })
}

func TestUpdateAndCancel(t *testing.T) {
	s := open(t, Config{})
	submit(t, s, "normal", nil)
	low := submit(t, s, "was-low", &Task{Priority: Low, Key: "k"})
	gone := submit(t, s, "cancelled", nil)
	if changed, err := s.Update(TaskRef{Key: "k"}, Update{Priority: Critical}); err != nil || !changed {
		t.Fatalf("update %v: %v", changed, err)
	}
	if info, err := s.Task(TaskRef{ID: low}); err != nil || info.Priority != Critical {
		t.Fatalf("task %+v: %v", info, err)
	}
	if ok, err := s.Cancel(TaskRef{ID: gone}); err != nil || !ok {
		t.Fatalf("cancel %v: %v", ok, err)
	}
	if ok, err := s.Cancel(TaskRef{ID: gone}); err != nil || ok {
		t.Fatalf("second cancel %v: %v", ok, err)
	}
	equal(t, drain(t, s), []string{"was-low", "normal"})
	if _, err := s.Update(TaskRef{ID: 999999}, Update{Priority: High}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update of nothing: %v", err)
	}
	if _, err := s.Task(TaskRef{ID: 999999}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("task of nothing: %v", err)
	}
}

func TestCancelEndsARunningLease(t *testing.T) {
	s := open(t, Config{})
	id := submit(t, s, "x", nil)
	l := next(t, s)
	if ok, err := s.Cancel(TaskRef{ID: id}); err != nil || !ok {
		t.Fatalf("cancel %v: %v", ok, err)
	}
	if err := l.Heartbeat(); !errors.Is(err, ErrLeaseGone) {
		t.Fatalf("heartbeat: %v", err)
	}
}

func TestResourcesAndErrors(t *testing.T) {
	s := open(t, Config{
		Pools:     []Pool{{Name: "default", MaxPending: 2}},
		Resources: []Resource{{Kind: "semaphore", Name: "uplink", Permits: 1}},
	})
	submit(t, s, "a", &Task{Resources: map[string]uint32{"uplink": 1}})
	submit(t, s, "b", &Task{Resources: map[string]uint32{"uplink": 1}})
	if _, err := s.Submit([]byte("c"), nil); !errors.Is(err, ErrFull) {
		t.Fatalf("full: %v", err)
	}
	a := next(t, s)
	if _, err := s.Next("", 50*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("second while the permit is held: %v", err)
	}
	if err := a.Complete(); err != nil {
		t.Fatal(err)
	}
	equal(t, drain(t, s), []string{"b"})
	if _, err := s.Submit(nil, &Task{Pool: "nope"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unknown pool: %v", err)
	}
	if _, err := s.Next("nope", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unknown pool: %v", err)
	}
	if _, err := Open(Config{Pools: []Pool{{Name: "a"}, {Name: "a"}}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("duplicate pool: %v", err)
	}
}

func TestManyGoroutines(t *testing.T) {
	s := open(t, Config{Pools: []Pool{{Name: "send"}, {Name: "upload", MaxLeases: 2}}})
	const n = 400
	for i := 0; i < n; i++ {
		pool := "send"
		if i%2 == 1 {
			pool = "upload"
		}
		submit(t, s, fmt.Sprint(i), &Task{Pool: pool})
	}
	var (
		mu   sync.Mutex
		done []string
		wg   sync.WaitGroup
	)
	for g := 0; g < 8; g++ {
		pool := "send"
		if g%2 == 1 {
			pool = "upload"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				l, err := s.Next(pool, 200*time.Millisecond)
				if err != nil {
					if !errors.Is(err, ErrTimeout) {
						t.Errorf("next: %v", err)
					}
					return
				}
				if l.Pool != pool {
					t.Errorf("lease of pool %q in %q", l.Pool, pool)
				}
				if err := l.Complete(); err != nil {
					t.Errorf("complete: %v", err)
				}
				mu.Lock()
				done = append(done, string(l.Payload))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(done) != n {
		t.Fatalf("%d tasks ran, want %d", len(done), n)
	}
	sort.Strings(done)
	for i := 1; i < len(done); i++ {
		if done[i] == done[i-1] {
			t.Fatalf("task %s ran twice", done[i])
		}
	}
	eventually(t, s, func(v *Snapshot) bool { return v.Totals["succeeded"] == n })
}

func TestShutdownWakesWaiters(t *testing.T) {
	s := open(t, Config{})
	got := make(chan error, 1)
	go func() {
		_, err := s.Next("", time.Minute)
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := s.Shutdown(0); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		if !errors.Is(err, ErrShutdown) {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting goroutine was not woken")
	}
	if _, err := s.Submit(nil, nil); !errors.Is(err, ErrShutdown) {
		t.Fatalf("submit after shutdown: %v", err)
	}
	s.Close()
	s.Close()
	if _, err := s.Snapshot(); !errors.Is(err, ErrShutdown) {
		t.Fatalf("snapshot after close: %v", err)
	}
}

func TestOutputAndWait(t *testing.T) {
	s := open(t, Config{Pools: []Pool{{Name: "default", Retry: &Retry{MaxAttempts: 1}}}})
	id := submit(t, s, "2+2", &Task{Key: "sum"})
	if _, err := s.Wait(TaskRef{ID: id}, 20*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("wait for a live task: %v", err)
	}
	got := make(chan *Result, 1)
	go func() {
		r, err := s.Wait(TaskRef{Key: "sum"}, 5*time.Second)
		if err != nil {
			t.Errorf("wait: %v", err)
		}
		got <- r
	}()
	if err := next(t, s).CompleteWithOutput([]byte("4\x00binary")); err != nil {
		t.Fatal(err)
	}
	if r := <-got; r == nil || !r.Succeeded || string(r.Output) != "4\x00binary" {
		t.Fatalf("result %+v", r)
	}
	bad := submit(t, s, "y", nil)
	if err := next(t, s).Fail("relay refused", true, 0); err != nil {
		t.Fatal(err)
	}
	r, err := s.Wait(TaskRef{ID: bad}, 5*time.Second)
	if err != nil || r.Succeeded || r.Message != "dead_lettered: relay refused" {
		t.Fatalf("result %+v: %v", r, err)
	}
	if _, err := s.Wait(TaskRef{ID: 999999}, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wait for nothing: %v", err)
	}
}

func TestEvents(t *testing.T) {
	s := open(t, Config{Events: "all"})
	ev, err := s.Events()
	if err != nil {
		t.Fatal(err)
	}
	defer ev.Close()
	if _, err := ev.Next(20 * time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("no event yet: %v", err)
	}
	id := submit(t, s, "x", &Task{Key: "k", Priority: High})
	if err := next(t, s).Complete(); err != nil {
		t.Fatal(err)
	}
	var seen []string
	for {
		e, err := ev.Next(300 * time.Millisecond)
		if errors.Is(err, ErrTimeout) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.Task != id || e.Key != "k" || e.Pool != "default" || e.Priority != High || e.AtUnixMs == 0 {
			t.Fatalf("event %+v", e)
		}
		seen = append(seen, e.Kind+":"+e.Status)
	}
	if len(seen) < 2 || seen[len(seen)-1] != "transition:succeeded" {
		t.Fatalf("events %v", seen)
	}
	// A reader is woken when the subscription is closed.
	done := make(chan error, 1)
	go func() {
		_, err := ev.Next(time.Minute)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	ev.Close()
	ev.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrShutdown) {
			t.Fatalf("reader: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the reader was not woken")
	}
}

func TestClockAfterAGap(t *testing.T) {
	no := false
	s := open(t, Config{Pools: []Pool{{Name: "default", Retry: &Retry{
		MaxAttempts: 2, InitialBackoffMs: 3600000, MaxBackoffMs: 3600000, Jitter: &no,
	}}}})
	id := submit(t, s, "x", nil)
	if err := next(t, s).Fail("later", true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next("", 50*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("retry before its backoff: %v", err)
	}
	if moved, err := s.ResyncClock(); err != nil || moved > time.Second {
		t.Fatalf("resync without a gap moved %v: %v", moved, err)
	}
	if err := s.AdvanceClock(time.Hour); err != nil {
		t.Fatal(err)
	}
	l, err := s.Next("", 2*time.Second)
	if err != nil || l.Task != id || l.Attempt != 2 {
		t.Fatalf("after the gap: %+v, %v", l, err)
	}
	if err := l.Complete(); err != nil {
		t.Fatal(err)
	}
	if moved, err := s.ResyncClock(); err != nil || moved != 0 {
		t.Fatalf("time moved back by a resync: %v, %v", moved, err)
	}
}
