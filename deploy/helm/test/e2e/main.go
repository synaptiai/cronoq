// Command e2e checks a cronoq release from inside the cluster: for each pool it runs a Go
// worker against the given server and submits a task through it, then waits for the result.
// Standbys answer UNAVAILABLE, so this also checks that clients and workers reach the leader
// through a Service that routes to every pod.
//
//	e2e -server http://r-cronoq:7070 -pools default,llm [-insecure-redirect-http]
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	cronoq "github.com/synaptiai/cronoq/sdks/go"
)

func main() {
	server := flag.String("server", "", "server endpoint")
	pools := flag.String("pools", "default", "comma-separated pools to check")
	insecure := flag.Bool("insecure-redirect-http", false, "let workers follow plain-http shard redirects")
	timeout := flag.Duration("timeout", 2*time.Minute, "overall deadline")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	c, err := cronoq.Dial(*server, cronoq.Options{Token: os.Getenv("CRONOQ_CLIENT_TOKEN")})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	for _, pool := range strings.Split(*pools, ",") {
		if err := check(ctx, c, *server, pool, *insecure); err != nil {
			log.Fatalf("pool %s: %v", pool, err)
		}
		log.Printf("pool %s: ok", pool)
	}
}

func check(ctx context.Context, c *cronoq.Client, server, pool string, insecure bool) error {
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- cronoq.RunWorker(wctx, cronoq.WorkerOptions{
			Target: server, Pool: pool, Token: os.Getenv("CRONOQ_WORKER_TOKEN"), Name: "e2e",
			InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, InsecureRedirectHTTP: insecure,
		}, func(_ context.Context, l *cronoq.Lease) ([]byte, error) {
			return bytes.ToUpper(l.Payload), nil
		})
	}()
	payload := fmt.Sprintf("hello %s %d", pool, time.Now().UnixNano())
	// A standby refuses with UNAVAILABLE; a new call may reach another pod.
	for {
		sub, err := c.SubmitAndWait(ctx, cronoq.Task{Pool: pool, Payload: []byte(payload)}, 20*time.Second)
		var e *cronoq.Error
		switch {
		case err == nil && sub.Result != nil && string(sub.Result.Output) == strings.ToUpper(payload):
			stop()
			return <-done
		case err == nil:
			return fmt.Errorf("unexpected result %+v", sub.Result)
		case errors.As(err, &e) && (e.Retryable() || e.Timeout()) && ctx.Err() == nil:
			log.Printf("pool %s: %v; retrying", pool, err)
			time.Sleep(500 * time.Millisecond)
		default:
			select {
			case werr := <-done:
				return fmt.Errorf("%w (worker: %v)", err, werr)
			default:
				return err
			}
		}
	}
}
