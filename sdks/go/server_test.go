package cronoq_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Tokens of the test servers (the server requires at least 16 characters).
const (
	clientToken = "client-token-0123456789"
	workerToken = "worker-token-0123456789"
	adminToken  = "admin-token-0123456789"
)

// server is a cronoq-server subprocess with fixed ports, so it can be restarted in place.
type server struct {
	t        *testing.T
	bin, dir string
	// serverLines go into the [server] table; tables follow the tokens and limits.
	serverLines, tables string
	grpcPort, httpPort  int
	cmd                 *exec.Cmd
	exited              chan struct{}
}

// serverBinary is $CRONOQ_SERVER_BIN, or the debug build in the repository's target directory.
func serverBinary(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("CRONOQ_SERVER_BIN")
	if bin == "" {
		name := "cronoq-server"
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		bin = filepath.Join("..", "..", "target", "debug", name)
	}
	if _, err := os.Stat(bin); err != nil {
		if os.Getenv("CRONOQ_REQUIRE_SERVER") != "" {
			t.Fatalf("cronoq-server not found at %s (build it with cargo build -p cronoq-server)", bin)
		}
		t.Skipf("cronoq-server not found at %s", bin)
	}
	return bin
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

const layout = `
[[auth.tokens]]
token = "` + clientToken + `"
role = "client"
[[auth.tokens]]
token = "` + workerToken + `"
role = "worker"
[[auth.tokens]]
token = "` + adminToken + `"
role = "admin"

[limits]
max_wait_s = 30
`

const defaultPool = `
[[pools]]
name = "default"
max = 8
lease_ttl_s = 1.0
retry = { max_attempts = 3, initial_backoff_s = 0.05, jitter = false }
`

// newServer configures a server with the default pool, or with tables in its place.
func newServer(t *testing.T, serverLines, tables string) *server {
	t.Helper()
	if tables == "" {
		tables = defaultPool
	}
	s := &server{t: t, bin: serverBinary(t), dir: t.TempDir(), serverLines: serverLines, tables: tables, grpcPort: freePort(t), httpPort: freePort(t)}
	t.Cleanup(s.stop)
	return s
}

func startServer(t *testing.T) *server {
	s := newServer(t, "", "")
	s.start()
	return s
}

func (s *server) target() string { return fmt.Sprintf("http://127.0.0.1:%d", s.grpcPort) }

func (s *server) start() {
	s.t.Helper()
	cfg := fmt.Sprintf("[server]\ngrpc_addr = \"127.0.0.1:%d\"\nhttp_addr = \"127.0.0.1:%d\"\nshutdown_grace_s = 0\n%s\n%s\n%s",
		s.grpcPort, s.httpPort, s.serverLines, layout, s.tables)
	path := filepath.Join(s.dir, "cronoq.toml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		s.t.Fatal(err)
	}
	logPath := filepath.Join(s.dir, "server.log")
	logf, err := os.Create(logPath)
	if err != nil {
		s.t.Fatal(err)
	}
	defer logf.Close()
	s.cmd = exec.Command(s.bin, "--config", path)
	s.cmd.Stderr = logf
	s.cmd.Env = append(os.Environ(), "RUST_LOG=warn")
	if err := s.cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	exited := make(chan struct{})
	s.exited = exited
	go func() { _ = s.cmd.Wait(); close(exited) }()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			log, _ := os.ReadFile(logPath)
			s.t.Fatalf("cronoq-server exited during startup: %s", log)
		default:
		}
		r, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", s.httpPort))
		if err == nil {
			r.Body.Close()
			if r.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.t.Fatal("cronoq-server did not become ready")
}

func (s *server) stop() {
	if s.cmd == nil {
		return
	}
	if runtime.GOOS == "windows" {
		_ = s.cmd.Process.Kill()
	} else {
		_ = s.cmd.Process.Signal(os.Interrupt)
	}
	select {
	case <-s.exited:
	case <-time.After(10 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.exited
	}
	s.cmd = nil
}
