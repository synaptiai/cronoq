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
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

grpc = pytest.importorskip("grpc")

import cronoq  # noqa: E402
from cronoq.client import CancelTask, Client, ClientError, Lease, Reject, Worker, task  # noqa: E402

# The server requires tokens of at least 16 characters.
CLIENT, WORKER, METRICS, ADMIN = (
    "client-token-0123456789",
    "worker-token-0123456789",
    "metrics-token-0123456789",
    "admin-token-0123456789",
)
#: Server-side caps used by the tests below.
MAX_SLOTS_PER_WORKER = 4
MAX_MESSAGE_BYTES = 256 * 1024
# The source checkout, when the tests run from it (wheel tests run from a copy elsewhere).
_HERE = Path(__file__).resolve()
REPO: Path | None = _HERE.parents[4] if len(_HERE.parents) > 4 else None


def _server_binary() -> Path | None:
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
        self.proc: subprocess.Popen | None = None

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
token = "{METRICS}"
role = "metrics"
[[auth.tokens]]
token = "{ADMIN}"
role = "admin"

[limits]
max_slots_per_worker = {MAX_SLOTS_PER_WORKER}
max_message_bytes = {MAX_MESSAGE_BYTES}
max_wait_s = 30

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
        self.workers: list[Worker] = []
        self.tasks: list[asyncio.Task] = []

    async def add(self, server: Server, handler: Any, **kw: Any) -> Worker:
        kw.setdefault("token", WORKER)
        kw.setdefault("backoff", (0.05, 0.2))
        w = Worker(server.target, "default", handler, **kw)
        self.workers.append(w)
        self.tasks.append(asyncio.create_task(w.run()))
        await w.wait_registered(10)
        return w

    async def __aenter__(self) -> Running:
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
        assert info.status == "succeeded" and info.done
        with pytest.raises(ClientError) as e:
            await client.get(10**15)
        assert e.value.code == "not_found"


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


async def test_labeled_tasks_go_to_labeled_workers(server: Server, client: Client) -> None:
    async def gpu(lease: Lease) -> Any:
        return "gpu"

    async def plain(lease: Lease) -> Any:
        return "plain"

    async with Running() as r:
        await r.add(server, plain)
        await r.add(server, gpu, labels={"gpu": "a100"})
        await wait_capacity(client, 2)
        for _ in range(3):
            sub = await client.submit("x", labels={"gpu": "a100"}, wait=True)
            assert sub.result.text() == "gpu"
        async with Client(client.target, token=ADMIN) as admin:
            groups = (await admin.snapshot()).pool("default").groups
        assert [dict(g.labels) for g in groups] == [{}, {"gpu": "a100"}]


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
    order: list[str] = []

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
    async with Client(server.target, token=METRICS) as metrics:
        snap = await metrics.snapshot()
        assert snap.totals.submitted >= 0 and snap.workers_connected == 0
    w = Worker(server.target, "default", upper, token=CLIENT)
    with pytest.raises(ClientError) as e:
        await asyncio.wait_for(w.run(), 10)
    assert e.value.code == "permission_denied"
    w = Worker(server.target, "nope", upper, token=WORKER)
    with pytest.raises(ClientError) as e:
        await asyncio.wait_for(w.run(), 10)
    assert e.value.code == "not_found"


async def test_timeout_aborts_running_handler(server: Server, client: Client) -> None:
    aborted: list[int] = []

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
    seen: list[str] = []

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
        # A graceful server stop (SIGTERM) announces itself before closing the stream. On
        # Windows the test stops the server with TerminateProcess, which is not graceful.
        if os.name != "nt":
            assert w.last_shutdown is not None
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


# ---------------------------------------------------------------------------------------------
# Regressions found in review


def test_after_accepts_a_single_key_or_id() -> None:
    assert [d.target.key for d in task("y", after="parent-key").deps] == ["parent-key"]
    assert [d.target.id for d in task("y", after=7).deps] == [7]
    assert [d.target.key for d in task("y", after=["a", "b"]).deps] == ["a", "b"]
    with pytest.raises(ValueError):
        task("y", deadline_in=1.0, deadline_at=time.time() + 5)
    with pytest.raises(ValueError):
        task("y", start_in=1.0, start_at=time.time() + 5)
    # The native `Dep(on=...)` condition names are accepted too.
    spec = task("y", deps=[("a", "failure"), {"ref": "b", "cond": "on_complete"}])
    pb = cronoq.client.pb
    assert [d.cond for d in spec.deps] == [pb.EDGE_COND_ON_FAILURE, pb.EDGE_COND_ON_COMPLETE]


async def test_handler_output_edge_cases(server: Server, client: Client) -> None:
    async def handler(lease: Lease) -> Any:
        if lease.text() == "none":
            return None
        if lease.text() == "set":
            return {1, 2}  # not JSON-serialisable
        return "ok"

    async with Running() as r:
        await r.add(server, handler)
        await wait_capacity(client, 1)
        none = (await client.submit("none", wait=True)).result
        assert none is not None and none.ok and none.json() is None
        bad = (await client.submit("set", wait=True)).result
        assert bad is not None and bad.status == "failed" and "JSON" in bad.message


async def test_handler_raising_cancelled_error_does_not_leak_the_slot(server: Server, client: Client) -> None:
    async def handler(lease: Lease) -> Any:
        if lease.text() == "selfcancel":
            fut = asyncio.get_running_loop().create_future()
            fut.cancel()
            await fut
        return "ok"

    async with Running() as r:
        w = await r.add(server, handler)
        await wait_capacity(client, 1)
        sub = await client.submit(
            "selfcancel", retry={"max_attempts": 2, "initial_backoff": 0.01}, wait=True, call_timeout=0.8
        )  # must not wait for the 1 s lease TTL
        res = sub.result
        assert res is not None and res.status == "dead_lettered" and "CancelledError" in res.message
        nxt = (await client.submit("next", wait=True)).result
        assert nxt is not None and nxt.ok
        assert w.running == 0


def test_client_and_worker_built_outside_a_loop(server: Server) -> None:
    c = Client(server.target, token=CLIENT)
    w = Worker(server.target, "default", upper, token=WORKER, backoff=(0.05, 0.2))

    async def use() -> None:
        with pytest.raises(ClientError):
            await c.get(10**15)
        run = asyncio.ensure_future(w.run())
        await w.wait_registered(10)
        await w.stop(1)
        await asyncio.wait_for(run, 10)

    asyncio.run(use())
    asyncio.run(use())  # both can be reused on a new event loop
    asyncio.run(c.close())


async def test_worker_stop_is_idempotent_and_drains(server: Server, client: Client) -> None:
    async def slow(lease: Lease) -> Any:
        await asyncio.sleep(0.3)
        return "done"

    w = Worker(server.target, "default", slow, slots=2, token=WORKER, backoff=(0.05, 0.2))
    run = asyncio.ensure_future(w.run())
    await w.wait_registered(10)
    await wait_capacity(client, 2)
    sub = await client.submit("x")
    for _ in range(200):
        if w.running:
            break
        await asyncio.sleep(0.01)
    # Two concurrent stops: both return only after the running lease finished.
    await asyncio.gather(w.stop(grace=5), w.stop(grace=5))
    assert w.running == 0
    assert (await client.wait(sub.id, timeout=5)).ok
    await asyncio.wait_for(run, 10)
    await w.stop()  # stopping a stopped worker is a no-op
    # The same worker can serve again after a stop.
    run = asyncio.ensure_future(w.run())
    await w.wait_registered(10)
    await wait_capacity(client, 2)
    assert (await client.submit("y", wait=True, call_timeout=10)).result.ok  # type: ignore[union-attr]
    await w.stop(grace=5)
    await asyncio.wait_for(run, 10)


async def test_wait_has_a_client_side_deadline(server: Server, client: Client) -> None:
    sub = await client.submit("never-runs")  # no workers connected
    t0 = time.monotonic()
    with pytest.raises(TimeoutError):
        await client.wait(sub.id, timeout=0.2)
    with pytest.raises(TimeoutError):
        await client.submit("x", wait=True, call_timeout=0.2)
    assert time.monotonic() - t0 < 5


def test_client_error_pickles() -> None:
    import pickle

    e = ClientError("not_found", "unknown task")
    back = pickle.loads(pickle.dumps(e))
    assert (back.code, back.message, str(back)) == (e.code, e.message, str(e))


# ---------------------------------------------------------------------------------------------
# Protocol version 2


async def test_get_returns_task_details(server: Server, client: Client) -> None:
    async with Running() as r:
        await r.add(server, upper)
        await wait_capacity(client, 1)
        sub = await client.submit("x", key="details", kind="upper", wait=True)
        info = await client.get("details")
    assert info.id == sub.id and info.status == "succeeded"
    assert (info.pool, info.key, info.kind, info.attempt) == ("default", "details", "upper", 1)
    assert info.max_attempts == 3
    assert info.created_at and info.finished_at and info.finished_at >= info.created_at
    assert info.output_available


async def test_list_tasks_workers_and_schedules(server: Server, client: Client) -> None:
    async with Running() as r:
        await r.add(server, upper, name="py-worker", labels={"zone": "eu"})
        await wait_capacity(client, 1)
        done = await client.submit("x", key="listed", tenant="acme", priority="high", wait=True)
        # No worker has gpu=a100, so this task stays queued.
        queued = await client.submit("y", labels={"gpu": "a100"})
        page = await client.list_tasks()
        assert [t.id for t in page.tasks] == [done.id, queued.id]
        assert page.next_after is None
        first = page.tasks[0]
        assert (first.status, first.key, first.tenant, first.priority) == ("succeeded", "listed", "acme", "high")
        assert [t.id for t in (await client.list_tasks(statuses=["ready"])).tasks] == [queued.id]
        one = await client.list_tasks(limit=1)
        assert [t.id for t in one.tasks] == [done.id] and one.next_after == done.id
        assert [t.id async for t in client.iter_tasks(page_size=1)] == [done.id, queued.id]
        with pytest.raises(ValueError):
            await client.list_tasks(statuses=["nope"])
        with pytest.raises(TypeError):
            await client.list_tasks(pools="default")

        async with Client(client.target, token=METRICS) as m:
            workers = await m.list_workers()
            assert [(w.name, w.pool, w.labels, w.slots) for w in workers] == [
                ("py-worker", "default", {"zone": "eu"}, 1)
            ]
            assert workers[0].connected_at > 0
        async with Client(client.target, token=ADMIN) as admin:
            await admin.add_recurring("hourly", "0 0 * * * *", "z", overlap="queue")
            (s,) = await admin.list_recurring()
            assert (s.name, s.cron, s.pool, s.overlap) == ("hourly", "0 0 * * * *", "default", "queue")
            assert s.next_at is not None and s.last_task_id is None
        with pytest.raises(ClientError) as e:
            await client.list_workers()
        assert e.value.code == "permission_denied"


async def test_submit_wait_timeout(client: Client) -> None:
    t0 = time.monotonic()
    with pytest.raises(TimeoutError):
        await client.submit("never-runs", wait=True, wait_timeout=0.2)  # no workers connected
    assert time.monotonic() - t0 < 5
    with pytest.raises(ValueError):
        await client.submit("x", wait_timeout=1)


async def test_watch_filters_and_transitions(server: Server, client: Client) -> None:
    async def collect(**kw: Any) -> list[str]:
        out: list[str] = []
        async for e in client.watch(**kw):
            out.append(e.status)
            if e.status == "succeeded":
                return out
        return out

    async with Running() as r:
        await r.add(server, upper)
        await wait_capacity(client, 1)
        terminal = asyncio.create_task(collect(keys=["t-1"]))
        everything = asyncio.create_task(collect(keys=["t-1"], all_transitions=True))
        other = asyncio.create_task(collect(keys=["t-2"]))
        await asyncio.sleep(0.2)
        await client.submit("x", key="t-1", wait=True)
        assert await asyncio.wait_for(terminal, 5) == ["succeeded"]
        statuses = await asyncio.wait_for(everything, 5)
        assert "running" in statuses and statuses[-1] == "succeeded"
        assert not other.done(), "filtered by key"
        other.cancel()


async def test_server_grants_capped_slots(server: Server, client: Client) -> None:
    gate = asyncio.Event()
    started: list[int] = []

    async def held(lease: Lease) -> Any:
        started.append(lease.task_id)
        await gate.wait()
        return "ok"

    async with Running() as r:
        w = await r.add(server, held, slots=MAX_SLOTS_PER_WORKER + 6)
        assert w.granted_slots == MAX_SLOTS_PER_WORKER
        assert w.server_protocol == 3 and w.max_message_bytes == MAX_MESSAGE_BYTES
        await wait_capacity(client, MAX_SLOTS_PER_WORKER)
        subs = [await client.submit(i) for i in range(MAX_SLOTS_PER_WORKER + 2)]
        for _ in range(200):
            if len(started) == MAX_SLOTS_PER_WORKER:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.2)
        assert len(started) == MAX_SLOTS_PER_WORKER and w.running == MAX_SLOTS_PER_WORKER
        gate.set()
        for sub in subs:
            assert (await client.wait(sub.id, timeout=10)).ok


async def test_reject_hands_the_lease_to_another_worker(server: Server, client: Client) -> None:
    refused: list[int] = []

    async def picky(lease: Lease) -> Any:
        refused.append(lease.task_id)
        raise Reject("not for me")

    async def willing(lease: Lease) -> Any:
        return f"done@{lease.attempt}"

    # A Nack is re-offered within the same attempt only while another worker has a free slot,
    # so both workers connect first and the willing one always has room for every task.
    async with Running() as r:
        await r.add(server, picky, name="picky", slots=1)
        await r.add(server, willing, name="willing", slots=MAX_SLOTS_PER_WORKER)
        await wait_capacity(client, 1 + MAX_SLOTS_PER_WORKER)
        subs = [await client.submit(i, retry={"max_attempts": 1}) for i in range(MAX_SLOTS_PER_WORKER - 1)]
        results = [await client.wait(s.id, timeout=10) for s in subs]
    assert all(res.ok and res.text() == "done@1" for res in results), results
    assert refused, "the picky worker was offered at least one lease"


async def test_cancel_task_from_the_handler(server: Server, client: Client) -> None:
    async def cancel(lease: Lease) -> Any:
        raise CancelTask("no longer needed")

    async with Running() as r:
        await r.add(server, cancel)
        await wait_capacity(client, 1)
        sub = await client.submit("x", wait=True)
    assert sub.result is not None and sub.result.status == "cancelled"


async def test_oversized_output_fails_permanently(server: Server, client: Client) -> None:
    async def big(lease: Lease) -> Any:
        return b"x" * (MAX_MESSAGE_BYTES + 1)

    async with Running() as r:
        w = await r.add(server, big)
        await wait_capacity(client, 1)
        sub = await client.submit("x", wait=True)
        assert sub.result is not None and sub.result.status == "failed"
        assert "message limit" in sub.result.message
        assert w.connections == 1, "the worker stays connected"


async def test_lease_metadata(server: Server, client: Client) -> None:
    seen: list[Lease] = []

    async def record(lease: Lease) -> Any:
        seen.append(lease)
        return "ok"

    async with Running() as r:
        await r.add(server, record)
        await wait_capacity(client, 1)
        await client.submit("x", wait=True)
    lease = seen[0]
    assert lease.pool == "default" and lease.lease_ttl == pytest.approx(1.0)
    assert lease.priority in (None, "normal")


def test_reject_and_cancel_task_pickle() -> None:
    import pickle

    assert pickle.loads(pickle.dumps(Reject("busy"))).reason == "busy"
    assert str(pickle.loads(pickle.dumps(CancelTask("stop")))) == "stop"


class ShardServer(Server):
    """One shard of a two-shard layout: ``cpu`` serves pool ``default``, ``gpu`` pool ``llm``."""

    def __init__(self, binary: Path, shard: str, ports: dict[str, int], layout: dict[str, list[str]]) -> None:
        self.shard, self.ports, self.layout = shard, ports, layout
        super().__init__(binary)

    def _new_ports(self) -> None:
        super()._new_ports()
        self.grpc_port = self.ports[self.shard]
        pools = "".join(f'\n[[pools]]\nname = "{p}"\nmax = 4\n' for p in ("default", "llm", "spare"))
        shards = "".join(
            f'\n[[shards]]\nname = "{name}"\nnumber = {n}\npools = {pools!r}\n'
            f'endpoint = "http://127.0.0.1:{self.ports[name]}"\n'.replace("'", '"')
            for n, (name, pools) in enumerate(self.layout.items(), start=1)
        )
        text = self.config.read_text()
        text = text.replace("[server]\n", f'[server]\nshard = "{self.shard}"\n', 1)
        text = text.replace('grpc_addr = "127.0.0.1:', f'grpc_addr = "127.0.0.1:{self.grpc_port}"\n#', 1)
        head, _, _ = text.partition("[[pools]]")
        self.config.write_text(head + pools + shards)


def _shards(binary: Path, layouts: dict[str, dict[str, list[str]]]) -> list[ShardServer]:
    ports = {name: _free_port() for name in layouts}
    servers = [ShardServer(binary, name, ports, layout) for name, layout in layouts.items()]
    for s in servers:
        s._start_once()
    return servers


async def test_worker_follows_its_pool_to_its_shard(server_binary: Path) -> None:
    layout = {"cpu": ["default", "spare"], "gpu": ["llm"]}
    servers = _shards(server_binary, {"cpu": layout, "gpu": layout})
    cpu = servers[0]
    try:
        # Pointed at cpu, the worker for pool `llm` registers with gpu and runs its tasks.
        w = Worker(cpu.target, "llm", upper, token=WORKER)
        run = asyncio.create_task(w.run())
        await w.wait_registered(10)
        assert w._current.endswith(f":{servers[1].grpc_port}")
        async with Client(cpu.target, token=CLIENT) as c:
            t = await c.submit("hi", pool="llm")
            assert (await c.wait(t.id, timeout=10)).text() == "HI"
        await w.stop()
        await run
    finally:
        for s in servers:
            s.stop()
            s.dir.cleanup()

    # Servers that each say the other serves `llm` send the worker back and forth until it stops.
    servers = _shards(
        server_binary,
        {"cpu": {"cpu": ["default", "spare"], "gpu": ["llm"]}, "gpu": {"cpu": ["default", "llm"], "gpu": ["spare"]}},
    )
    try:
        w = Worker(servers[0].target, "llm", upper, token=WORKER)
        with pytest.raises(ClientError, match="redirected more than 3 times"):
            await asyncio.wait_for(w.run(), 20)
    finally:
        for s in servers:
            s.stop()
            s.dir.cleanup()


def test_redirects_never_weaken_the_connection() -> None:
    from cronoq.client import _redirect

    class Refusal(grpc.aio.AioRpcError):
        def __init__(self, code: grpc.StatusCode, endpoint: str | None) -> None:
            md = grpc.aio.Metadata(*([("cronoq-shard-endpoint", endpoint)] if endpoint else []))
            super().__init__(code, grpc.aio.Metadata(), md, "pool 'llm' is served by shard 'gpu'")

    fp = grpc.StatusCode.FAILED_PRECONDITION
    assert _redirect(Refusal(fp, "https://gpu:7070"), True, True, False) == "https://gpu:7070"
    assert _redirect(Refusal(grpc.StatusCode.NOT_FOUND, "https://gpu:7070"), True, True, False) is None
    assert _redirect(Refusal(fp, None), True, True, False) is None
    for bad in ("http://127.0.0.1:7070", "unix:/tmp/s"):
        with pytest.raises(ClientError):
            _redirect(Refusal(fp, bad), True, True, False)
    # From plain http: loopback is fine, another host only without a token or with the opt-in.
    assert _redirect(Refusal(fp, "http://127.0.0.1:7071"), False, True, False)
    with pytest.raises(ClientError, match="insecure_redirect_http"):
        _redirect(Refusal(fp, "http://gpu.internal:7070"), False, True, False)
    assert _redirect(Refusal(fp, "http://gpu.internal:7070"), False, False, False)
    assert _redirect(Refusal(fp, "http://gpu.internal:7070"), False, True, True)
