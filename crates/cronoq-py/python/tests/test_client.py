"""End-to-end tests for cronoq.client against a real cronoq-server process."""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterator, List, Optional

import pytest

grpc = pytest.importorskip("grpc")

import cronoq  # noqa: E402
from cronoq.client import Client, ClientError, Lease, Worker, task  # noqa: E402

CLIENT, WORKER, ADMIN = "client-token", "worker-token", "admin-token"
# The source checkout, when the tests run from it (wheel tests run from a copy elsewhere).
_HERE = Path(__file__).resolve()
REPO: Optional[Path] = _HERE.parents[4] if len(_HERE.parents) > 4 else None


def _server_binary() -> Optional[Path]:
    env = os.environ.get("CRONOQ_SERVER_BIN")
    if env:
        return Path(env)
    if REPO is None or not (REPO / "Cargo.toml").exists():
        return None
    target = Path(os.environ.get("CARGO_TARGET_DIR", REPO / "target"))
    binary = target / "debug" / ("cronoq-server.exe" if os.name == "nt" else "cronoq-server")
    if not binary.exists():
        try:
            subprocess.run(
                ["cargo", "build", "-q", "-p", "cronoq-server"], cwd=REPO, check=True, timeout=1800
            )
        except (OSError, subprocess.SubprocessError):
            return None
    return binary if binary.exists() else None


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """A cronoq-server subprocess with fixed ports (so it can be restarted in place)."""

    def __init__(self, binary: Path) -> None:
        self.binary = binary
        self.dir = tempfile.TemporaryDirectory()
        self.config = Path(self.dir.name) / "cronoq.toml"
        self._new_ports()
        self.proc: Optional[subprocess.Popen] = None

    def _new_ports(self) -> None:
        self.grpc_port, self.http_port = _free_port(), _free_port()
        self.config.write_text(
            f"""
[server]
grpc_addr = "127.0.0.1:{self.grpc_port}"
http_addr = "127.0.0.1:{self.http_port}"
shutdown_grace_s = 0

[[auth.tokens]]
token = "{CLIENT}"
role = "client"
[[auth.tokens]]
token = "{WORKER}"
role = "worker"
[[auth.tokens]]
token = "{ADMIN}"
role = "admin"

[[pools]]
name = "default"
max = 8
lease_ttl_s = 1.0
retry = {{ max_attempts = 3, initial_backoff_s = 0.05, jitter = false }}
"""
        )

    @property
    def target(self) -> str:
        return f"127.0.0.1:{self.grpc_port}"

    def start(self, attempts: int = 3) -> None:
        """Start the server. Free ports can be taken by another process between probing and
        binding, so a failed first start retries on fresh ports (restarts reuse the ports)."""
        for attempt in range(attempts):
            try:
                return self._start_once()
            except RuntimeError:
                if attempt == attempts - 1:
                    raise
                self._new_ports()

    def _start_once(self) -> None:
        log = Path(self.dir.name) / "server.log"
        self.proc = subprocess.Popen(
            [str(self.binary), "--config", str(self.config)],
            stdout=subprocess.DEVNULL,
            stderr=log.open("wb"),
            env={**os.environ, "RUST_LOG": "warn"},
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"cronoq-server exited during startup: {log.read_text()[-2000:]}")
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.http_port}/readyz", timeout=1) as r:
                    if r.status == 200:
                        return
            except (urllib.error.URLError, ConnectionError, OSError):
                pass
            time.sleep(0.05)
        raise RuntimeError("cronoq-server did not become ready")

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None


@pytest.fixture(scope="session")
def server_binary() -> Path:
    binary = _server_binary()
    if binary is None:
        if os.environ.get("CRONOQ_REQUIRE_SERVER"):
            pytest.fail("cronoq-server binary not available and CRONOQ_REQUIRE_SERVER is set")
        pytest.skip("cronoq-server binary not available (cargo build failed)")
    return binary


@pytest.fixture
def server(server_binary: Path) -> Iterator[Server]:
    s = Server(server_binary)
    s.start()
    try:
        yield s
    finally:
        s.stop()
        s.dir.cleanup()


async def upper(lease: Lease) -> Any:
    return lease.payload.upper()


class Running:
    """Start workers in the background and stop them on exit."""

    def __init__(self) -> None:
        self.workers: List[Worker] = []
        self.tasks: List[asyncio.Task] = []

    async def add(self, server: Server, handler: Any, **kw: Any) -> Worker:
        kw.setdefault("token", WORKER)
        kw.setdefault("backoff", (0.05, 0.2))
        w = Worker(server.target, "default", handler, **kw)
        self.workers.append(w)
        self.tasks.append(asyncio.create_task(w.run()))
        await w.wait_registered(10)
        return w

    async def __aenter__(self) -> "Running":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        for w in self.workers:
            await w.stop(grace=1)
        await asyncio.wait_for(asyncio.gather(*self.tasks, return_exceptions=True), 15)


async def wait_capacity(c: Client, n: int) -> None:
    async with Client(c.target, token=ADMIN) as admin:
        for _ in range(400):
            snap = await admin.snapshot()
            if snap.pool("default").capacity == n:
                return
            await asyncio.sleep(0.025)
    raise AssertionError(f"capacity never reached {n}")


@pytest.fixture
async def client(server: Server) -> Any:
    c = Client(server.target, token=CLIENT)
    yield c
    await c.close()


async def test_submit_and_wait_through_python_worker(server: Server, client: Client) -> None:
    async with Running() as r:
        await r.add(server, upper, slots=2)
        await wait_capacity(client, 2)
        sub = await client.submit("hello", wait=True)
        assert not sub.duplicate
        assert sub.result is not None and sub.result.ok
        assert sub.result.text() == "HELLO"
        info = await client.get(sub.id)
        assert info is not None and info.status == "succeeded"
        assert await client.get(10**15) is None


async def test_json_payload_round_trip(server: Server, client: Client) -> None:
    async def add(lease: Lease) -> Any:
        body = lease.json()
        assert lease.content_type == "application/json"
        return {"sum": body["a"] + body["b"], "kind": lease.kind, "tags": lease.tags}

    async with Running() as r:
        await r.add(server, add)
        await wait_capacity(client, 1)
        sub = await client.submit({"a": 2, "b": 3}, kind="adder", tags={"team": "x"}, wait=True)
        assert sub.result.json() == {"sum": 5, "kind": "adder", "tags": {"team": "x"}}


async def test_failure_mapping(server: Server, client: Client) -> None:
    async def handler(lease: Lease) -> Any:
        text = lease.text()
        if text == "permanent":
            raise cronoq.Permanent("bad input")
        if text == "flaky" and lease.attempt < 2:
            raise cronoq.Retryable("try again", retry_after=0.05)
        if text == "boom" and lease.attempt < 3:
            raise RuntimeError("unexpected")
        return f"{text}@{lease.attempt}"

    async with Running() as r:
        await r.add(server, handler, slots=3)
        await wait_capacity(client, 3)
        perm, flaky, boom = await asyncio.gather(
            client.submit("permanent", wait=True),
            client.submit("flaky", wait=True),
            client.submit("boom", wait=True),
        )
        assert perm.result.status == "failed" and "bad input" in perm.result.message
        assert flaky.result.text() == "flaky@2"
        assert boom.result.text() == "boom@3", "generic exceptions are retryable"


async def test_cancel_queued_task(client: Client) -> None:
    # No workers are connected, so the task stays queued.
    sub = await client.submit("never", key="cancel-me")
    assert (await client.get("cancel-me")).status == "ready"
    assert await client.cancel("cancel-me")
    result = await client.wait(sub.id, timeout=5)
    assert result.status == "cancelled" and result.output is None
    assert not await client.cancel("cancel-me")


async def test_wait_timeout(client: Client) -> None:
    sub = await client.submit("never")
    with pytest.raises(TimeoutError):
        await client.wait(sub.id, timeout=0.1)


async def test_batch_with_key_dependencies(server: Server, client: Client) -> None:
    order: List[str] = []

    async def record(lease: Lease) -> Any:
        await asyncio.sleep(0.02)
        order.append(lease.key)
        return lease.key

    async with Running() as r:
        await r.add(server, record, slots=4)
        await wait_capacity(client, 4)
        subs = await client.submit_batch(
            [
                task("c", key="c", after=["a", "b"]),
                task("b", key="b", after=["a"]),
                task("a", key="a"),
                task("on-fail", key="d", deps=[("a", "on_failure")]),
            ]
        )
        assert len(subs) == 4
        assert (await client.wait("c", timeout=10)).text() == "c"
        assert (await client.wait("d", timeout=10)).status == "skipped"
        assert order == ["a", "b", "c"]
        dup = await client.submit("again", key="a")
        assert dup.duplicate and dup.id == subs[2].id


async def test_watch_streams_events(server: Server, client: Client) -> None:
    events = []

    async def collect() -> None:
        async for e in client.watch(pools=["default"]):
            events.append(e)
            if e.status == "succeeded":
                return

    async with Running() as r:
        await r.add(server, upper)
        await wait_capacity(client, 1)
        watcher = asyncio.create_task(collect())
        await asyncio.sleep(0.1)
        sub = await client.submit("watched", key="w1", wait=True)
        await asyncio.wait_for(watcher, 5)
    assert events[-1].id == sub.id and events[-1].key == "w1" and events[-1].pool == "default"
    assert events[-1].at > 1e9


async def test_auth_errors(server: Server) -> None:
    async with Client(server.target, token="wrong") as bad:
        with pytest.raises(ClientError) as e:
            await bad.submit("x")
        assert e.value.code == "unauthenticated"
    async with Client(server.target, token=WORKER) as worker_token:
        with pytest.raises(ClientError) as e:
            await worker_token.submit("x")
        assert e.value.code == "permission_denied"
    async with Client(server.target, token=CLIENT) as client:
        with pytest.raises(ClientError) as e:
            await client.snapshot()
        assert e.value.code == "permission_denied"
        with pytest.raises(ClientError) as e:
            await client.submit("x", pool="missing")
        assert e.value.code == "not_found"
    async with Client(server.target, token=ADMIN) as admin:
        snap = await admin.snapshot()
        assert snap.pool("default") is not None
    w = Worker(server.target, "default", upper, token=CLIENT)
    with pytest.raises(ClientError) as e:
        await asyncio.wait_for(w.run(), 10)
    assert e.value.code == "permission_denied"
    w = Worker(server.target, "nope", upper, token=WORKER)
    with pytest.raises(ClientError) as e:
        await asyncio.wait_for(w.run(), 10)
    assert e.value.code == "not_found"


async def test_timeout_aborts_running_handler(server: Server, client: Client) -> None:
    aborted: List[int] = []

    async def slow(lease: Lease) -> Any:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            assert lease.cancelled.is_set()
            aborted.append(lease.attempt)
            raise
        return b""

    async with Running() as r:
        w = await r.add(server, slow)
        await wait_capacity(client, 1)
        sub = await client.submit("slow", timeout=0.3, retry={"max_attempts": 2, "initial_backoff": 0.05}, wait=True)
        assert sub.result.status == "dead_lettered"
        for _ in range(100):
            if len(aborted) == 2:
                break
            await asyncio.sleep(0.02)
        assert aborted == [1, 2]
        assert w.running == 0


async def test_heartbeats_keep_long_leases_alive(server: Server, client: Client) -> None:
    async def long(lease: Lease) -> Any:
        await asyncio.sleep(2.5)  # > lease_ttl of 1s
        return "done"

    async with Running() as r:
        await r.add(server, long)
        await wait_capacity(client, 1)
        sub = await client.submit("x", retry={"max_attempts": 1}, wait=True)
        assert sub.result.text() == "done"


async def test_two_workers_share_load(server: Server, client: Client) -> None:
    seen: List[str] = []

    def make(name: str) -> Any:
        async def h(lease: Lease) -> Any:
            seen.append(name)
            await asyncio.sleep(0.2)
            return name

        return h

    async with Running() as r:
        await r.add(server, make("w1"), name="w1")
        await r.add(server, make("w2"), name="w2")
        await wait_capacity(client, 2)
        t0 = time.monotonic()
        results = await asyncio.gather(*(client.submit(i, wait=True) for i in range(6)))
        elapsed = time.monotonic() - t0
        assert all(s.result.ok for s in results)
        assert set(seen) == {"w1", "w2"}
        assert elapsed < 1.1, f"6 x 0.2s on 2 workers took {elapsed:.2f}s"


async def test_graceful_stop_finishes_running_leases(server: Server, client: Client) -> None:
    started = asyncio.Event()

    async def h(lease: Lease) -> Any:
        started.set()
        await asyncio.sleep(0.3)
        return "finished"

    w = Worker(server.target, "default", h, token=WORKER)
    run = asyncio.create_task(w.run())
    await w.wait_registered(10)
    await wait_capacity(client, 1)
    sub = await client.submit("x")
    await asyncio.wait_for(started.wait(), 5)
    await w.stop(grace=5)
    await asyncio.wait_for(run, 10)
    result = await client.wait(sub.id, timeout=5)
    assert result.text() == "finished"
    await wait_capacity(client, 0)


async def test_worker_reconnects_after_server_restart(server: Server) -> None:
    async with Running() as r:
        w = await r.add(server, upper)
        assert w.connections == 1
        server.stop()
        await asyncio.sleep(0.3)
        server.start(attempts=1)  # restart in place: keep the same ports
        await asyncio.wait_for(w.wait_registered(), 15)
        for _ in range(200):
            if w.connections >= 2 and w._registered.is_set():  # type: ignore[attr-defined]
                break
            await asyncio.sleep(0.05)
        assert w.connections >= 2
        async with Client(server.target, token=CLIENT) as c:
            await wait_capacity(c, 1)
            sub = await c.submit("after restart", wait=True)
            assert sub.result.text() == "AFTER RESTART"


def test_task_spec_builder() -> None:
    spec = task(
        b"\x00\x01",
        pool="p",
        kind="k",
        priority=cronoq.Priority.CRITICAL,
        key="key",
        deadline_in=5,
        start_at=1_700_000_000.5,
        tolerance=2,
        on_miss="skip",
        timeout=1.5,
        retry=cronoq.RetryPolicy(max_attempts=4),
        after=[7, "a"],
        deps=[{"ref": "b", "cond": "on_complete"}],
        resources={"gpu": 1},
        tags={"t": "v"},
    )
    assert spec.content_type == "application/octet-stream"
    assert spec.priority == 1  # PRIORITY_CRITICAL
    assert spec.start_unix_ns == 1_700_000_000_500_000_000
    assert spec.retry.max_attempts == 4
    assert [d.target.WhichOneof("target") for d in spec.deps] == ["id", "key", "key"]
    assert spec.resources["gpu"] == 1
    assert task("hi").content_type.startswith("text/plain")
    with pytest.raises(ValueError):
        task(1, priority="urgent")
    with pytest.raises(ValueError):
        task(1, deps=[("x", "sometimes")])
