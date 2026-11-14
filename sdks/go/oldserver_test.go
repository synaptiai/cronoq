package cronoq_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	cronoq "github.com/synaptiai/cronoq/sdks/go"
	pb "github.com/synaptiai/cronoq/sdks/go/cronoqpb"
)

// oldTasks answers a submission like a 0.2.2 server: it does not know on_key_conflict,
// returns the task that holds a key as a duplicate, and sets no outcome.
type oldTasks struct {
	pb.UnimplementedTasksServer
}

func (oldTasks) Submit(_ context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	// Key "taken" belongs to task 7; any other key is free.
	if req.GetTask().GetKey() == "taken" {
		return &pb.SubmitResponse{Id: 7, Duplicate: true}, nil
	}
	return &pb.SubmitResponse{Id: 8}, nil
}

func TestRejectConflictIsNotSilentlyIgnoredByAnOlderServer(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterTasksServer(srv, oldTasks{})
	go srv.Serve(l)
	t.Cleanup(srv.Stop)
	c, err := cronoq.Dial("http://"+l.Addr().String(), cronoq.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	ctx := ctxFor(t)

	// The key is in use, or free and now taken by this submission: either way the old server
	// did not compare anything, and the caller is told.
	for _, key := range []string{"taken", "free"} {
		var e *cronoq.Error
		_, err := c.Submit(ctx, cronoq.Task{Payload: []byte("edit-2"), Key: key, RejectConflict: true})
		if !errors.As(err, &e) || e.Code != codes.Unimplemented || e.KeyConflict() {
			t.Fatalf("%s: %+v", key, err)
		}
	}
	// Without the option the same answer is fine.
	plain, err := c.Submit(ctx, cronoq.Task{Payload: []byte("edit-2"), Key: "taken"})
	if err != nil || plain.ID != 7 || !plain.Duplicate {
		t.Fatalf("plain = %+v, %v", plain, err)
	}
}
