package cronoq

import (
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func refusal(code codes.Code, endpoint string) error {
	md := metadata.MD{}
	if endpoint != "" {
		md.Set(endpointHeader, endpoint)
	}
	return trailerError{status.Error(code, "pool 'llm' is served by shard 'gpu'"), md}
}

func TestRedirectsNeverWeakenTheConnection(t *testing.T) {
	fp := codes.FailedPrecondition
	opts := &WorkerOptions{Target: "https://cpu.internal:7070", Token: "t", Logger: slog.Default()}
	follow := func(o *WorkerOptions, current string, err error) (string, error) {
		return redirect(o, current, err)
	}
	if to, err := follow(opts, opts.Target, refusal(fp, "https://gpu.internal:7070")); err != nil || to != "https://gpu.internal:7070" {
		t.Fatalf("https redirect: %q, %v", to, err)
	}
	// Not a redirect: another code, or no endpoint.
	if to, err := follow(opts, opts.Target, refusal(codes.NotFound, "https://gpu:1")); to != "" || err != nil {
		t.Fatalf("not found: %q, %v", to, err)
	}
	if to, err := follow(opts, opts.Target, refusal(fp, "")); to != "" || err != nil {
		t.Fatalf("no endpoint: %q, %v", to, err)
	}
	// No downgrade from https, not even to loopback; no other schemes, no userinfo.
	for _, bad := range []string{"http://127.0.0.1:7070", "unix:/tmp/s", "https://u:p@gpu:7070", "https://gpu:7070/path"} {
		if _, err := follow(opts, opts.Target, refusal(fp, bad)); err == nil {
			t.Errorf("followed %q", bad)
		}
	}

	// From plain http with a token: loopback and https are fine, another plain-http host is not.
	local := &WorkerOptions{Target: "http://127.0.0.1:7070", Token: "t", Logger: slog.Default()}
	for target, ok := range map[string]bool{
		"http://127.0.0.1:7071":                true,
		"http://[::1]:7071":                    true,
		"http://localhost:7071":                true,
		"https://gpu.internal:7070":            true,
		"http://gpu.internal:7070":             false,
		"http://127.0.0.1:1@gpu.internal:7070": false,
	} {
		_, err := follow(local, local.Target, refusal(fp, target))
		if (err == nil) != ok {
			t.Errorf("%s: err = %v", target, err)
		}
	}
	// A plain-http endpoint on another host does not by itself allow more of them; the opt-in
	// does, and so does having no token to leak.
	remote := &WorkerOptions{Target: "http://cpu.internal:7070", Token: "t", Logger: slog.Default()}
	if _, err := follow(remote, remote.Target, refusal(fp, "http://gpu.internal:7070")); err == nil {
		t.Error("followed plain http to another host with a token")
	}
	remote.InsecureRedirectHTTP = true
	if _, err := follow(remote, remote.Target, refusal(fp, "http://gpu.internal:7070")); err != nil {
		t.Errorf("opt-in: %v", err)
	}
	anon := &WorkerOptions{Target: "http://127.0.0.1:7070", Logger: slog.Default()}
	if _, err := follow(anon, anon.Target, refusal(fp, "http://gpu.internal:7070")); err != nil {
		t.Errorf("no token: %v", err)
	}
}

func TestTargets(t *testing.T) {
	for target, want := range map[string]struct {
		addr   string
		secure bool
		ok     bool
	}{
		"http://127.0.0.1:7070":  {"127.0.0.1:7070", false, true},
		"https://cronoq:443/":    {"cronoq:443", true, true},
		"127.0.0.1:7070":         {"127.0.0.1:7070", false, true},
		"grpc://127.0.0.1:7070":  {ok: false},
		"http://u@cronoq:7070":   {ok: false},
		"http://cronoq":          {ok: false},
		"https://cronoq:1/x?y=z": {ok: false},
	} {
		addr, secure, err := endpoint(target)
		if (err == nil) != want.ok || (want.ok && (addr != want.addr || secure != want.secure)) {
			t.Errorf("%s: %q %v %v", target, addr, secure, err)
		}
	}
}
