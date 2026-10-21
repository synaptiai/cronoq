"""Client and remote worker for a ``cronoq-server`` over gRPC.

Requires the ``client`` extra: ``pip install "cronoq[client]"``.

Submitting work::

    from cronoq.client import Client

    async with Client("localhost:7070", token="client-token") as c:
        sub = await c.submit({"prompt": "hi"}, pool="llm", priority="high", wait=True)
        print(sub.result.json())

Serving a pool::

    from cronoq.client import Worker

    async def handle(lease):
        return {"echo": lease.json()}

    await Worker("localhost:7070", "llm", handle, slots=8, token="worker-token").run()
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

try:
    import grpc
    import grpc.aio
except ImportError as e:  # pragma: no cover - exercised only without the extra
    raise ImportError('cronoq.client requires grpcio and protobuf: pip install "cronoq[client]"') from e

from ._proto import cronoq_pb2 as pb
from ._proto import cronoq_pb2_grpc as pb_grpc
from .errors import CronoqError, Permanent, Retryable

__all__ = [
    "Client",
    "ClientError",
    "Event",
    "Lease",
    "PoolSnapshot",
    "Result",
    "Snapshot",
    "Submitted",
    "TaskInfo",
    "Worker",
    "task",
]

log = logging.getLogger("cronoq.client")

Payload = Union[bytes, bytearray, memoryview, str, dict, list, int, float, bool, None]
TaskRef = Union[int, str]


class ClientError(CronoqError):
    """A gRPC call failed. ``code`` is the lower-case gRPC status name, e.g.
    ``unauthenticated``, ``permission_denied``, ``not_found``, ``resource_exhausted``
    (queue full), ``failed_precondition`` (reservation conflict), ``invalid_argument``,
    ``unavailable``."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message

    def __reduce__(self) -> Tuple[Any, ...]:
        return (type(self), (self.code, self.message))

    @classmethod
    def from_rpc(cls, e: grpc.RpcError) -> "ClientError":
        code = e.code() if hasattr(e, "code") else None
        details = e.details() if hasattr(e, "details") else str(e)
        return cls(code.name.lower() if code is not None else "unknown", details or "")


def _status_name(value: int) -> str:
    name = pb.TaskStatus.Name(value)
    return name[len("TASK_STATUS_"):].lower() if name.startswith("TASK_STATUS_") else name.lower()


# ---------------------------------------------------------------------------------------------
# Results


def _decode_json(data: Optional[bytes]) -> Any:
    if data is None:
        raise ValueError("no output")
    if not data:
        return None  # a handler that returned None
    return json.loads(data)


@dataclass(frozen=True)
class Result:
    """Outcome of a finished task."""

    id: int
    status: str
    output: Optional[bytes]
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "succeeded"

    def json(self) -> Any:
        """Decode the output as JSON (``None`` for an empty output). Raises ``ValueError`` if
        the task produced no output."""
        return _decode_json(self.output)

    def text(self) -> str:
        return (self.output or b"").decode()

    @classmethod
    def _from_pb(cls, r: "pb.TaskResult") -> "Result":
        return cls(
            id=r.id,
            status=_status_name(r.status),
            output=r.output if r.HasField("output") else None,
            message=r.message,
        )


@dataclass(frozen=True)
class Submitted:
    id: int
    duplicate: bool
    result: Optional[Result] = None


@dataclass(frozen=True)
class TaskInfo:
    id: int
    status: str


@dataclass(frozen=True)
class Event:
    """A task status transition."""

    id: int
    pool: str
    key: str
    status: str
    attempt: int
    at: float  # Unix seconds
    message: str


@dataclass(frozen=True)
class PoolSnapshot:
    name: str
    capacity: int
    busy: int
    held: int
    desired: int
    pending_by_class: Tuple[int, ...]
    reservations: int
    dispatched: int
    succeeded: int
    failed: int
    priority_adherence: float
    fixed_start_hit_rate: float
    utilization: float
    schedule_efficiency: float
    wait_p50: float
    wait_p90: float
    wait_p99: float
    cost: float

    @property
    def pending(self) -> int:
        return sum(self.pending_by_class)


@dataclass(frozen=True)
class Snapshot:
    pools: Tuple[PoolSnapshot, ...]
    live_tasks: int
    running: int
    pending: int

    def pool(self, name: str) -> Optional[PoolSnapshot]:
        return next((p for p in self.pools if p.name == name), None)


# ---------------------------------------------------------------------------------------------
# Building task specs


_PRIORITIES = {
    "critical": pb.PRIORITY_CRITICAL,
    "high": pb.PRIORITY_HIGH,
    "normal": pb.PRIORITY_NORMAL,
    "low": pb.PRIORITY_LOW,
}
_PRIORITY_BY_INDEX = [pb.PRIORITY_CRITICAL, pb.PRIORITY_HIGH, pb.PRIORITY_NORMAL, pb.PRIORITY_LOW]
_MISS = {"run_late": pb.MISS_POLICY_RUN_LATE, "skip": pb.MISS_POLICY_SKIP, "fail": pb.MISS_POLICY_FAIL}
_CONDS = {
    "on_success": pb.EDGE_COND_ON_SUCCESS,
    "on_complete": pb.EDGE_COND_ON_COMPLETE,
    "on_failure": pb.EDGE_COND_ON_FAILURE,
    # The spellings used by the native `cronoq.Dep(on=...)`.
    "success": pb.EDGE_COND_ON_SUCCESS,
    "complete": pb.EDGE_COND_ON_COMPLETE,
    "failure": pb.EDGE_COND_ON_FAILURE,
}


def _priority(p: Any) -> int:
    if p is None:
        return pb.PRIORITY_NORMAL
    if isinstance(p, str):
        try:
            return _PRIORITIES[p.lower()]
        except KeyError:
            raise ValueError(f"unknown priority {p!r}") from None
    # cronoq.Priority (native enum, int() = class index 0..3) or a plain int index.
    i = int(p)
    if not 0 <= i < 4:
        raise ValueError(f"unknown priority {p!r}")
    return _PRIORITY_BY_INDEX[i]


def _encode(payload: Payload, content_type: Optional[str]) -> Tuple[bytes, str]:
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload), content_type or "application/octet-stream"
    if isinstance(payload, str):
        return payload.encode(), content_type or "text/plain; charset=utf-8"
    if payload is None:
        return b"", content_type or ""
    return json.dumps(payload).encode(), content_type or "application/json"


def _ref(r: TaskRef) -> "pb.TaskRef":
    if isinstance(r, bool):
        raise TypeError("task reference must be an id (int) or key (str)")
    if isinstance(r, int):
        return pb.TaskRef(id=r)
    if isinstance(r, str):
        return pb.TaskRef(key=r)
    raise TypeError("task reference must be an id (int) or key (str)")


def _unix_ns(t: Union[datetime, float, int]) -> int:
    if isinstance(t, datetime):
        return int(t.timestamp() * 1e9)
    return int(float(t) * 1e9)


def _retry(r: Any) -> "pb.RetryPolicy":
    if isinstance(r, Mapping):
        get = r.get
    else:
        def get(k: str, d: Any = None) -> Any:
            return getattr(r, k, d)

    return pb.RetryPolicy(
        max_attempts=int(get("max_attempts", 3)),
        initial_backoff_s=float(get("initial_backoff", 0.0) or 0.0),
        multiplier=float(get("multiplier", 0.0) or 0.0),
        max_backoff_s=float(get("max_backoff", 0.0) or 0.0),
        jitter=bool(get("jitter", True)),
    )


def task(
    payload: Payload = None,
    *,
    pool: Optional[str] = None,
    kind: Optional[str] = None,
    priority: Any = None,
    key: Optional[str] = None,
    deadline_in: Optional[float] = None,
    deadline_at: Union[datetime, float, None] = None,
    start_in: Optional[float] = None,
    start_at: Union[datetime, float, None] = None,
    tolerance: Optional[float] = None,
    on_miss: Optional[str] = None,
    duration_hint: Optional[float] = None,
    timeout: Optional[float] = None,
    retry: Any = None,
    after: Union[TaskRef, Iterable[TaskRef], None] = None,
    deps: Iterable[Union[Tuple[TaskRef, str], Mapping[str, Any]]] = (),
    resources: Optional[Mapping[str, int]] = None,
    tags: Optional[Mapping[str, str]] = None,
    content_type: Optional[str] = None,
) -> "pb.TaskSpec":
    """Build a wire task spec (for :meth:`Client.submit_batch` and :meth:`Client.add_recurring`).

    Times are seconds: ``*_in`` relative to now, ``*_at`` absolute (Unix seconds or a
    timezone-aware ``datetime``); give at most one of each pair. ``after`` is a key, an id or a
    list of them (each must succeed first). ``deps`` entries are ``(ref, cond)`` tuples or
    ``{"ref": ..., "cond": ...}`` with ``cond`` in ``success``/``complete``/``failure`` (or the
    ``on_``-prefixed spellings).
    """
    if deadline_in is not None and deadline_at is not None:
        raise ValueError("use either deadline_in or deadline_at")
    if start_in is not None and start_at is not None:
        raise ValueError("use either start_in or start_at")
    data, ct = _encode(payload, content_type)
    spec = pb.TaskSpec(
        payload=data,
        content_type=ct,
        kind=kind or "",
        pool=pool or "",
        priority=_priority(priority),
        key=key or "",
    )
    if deadline_in is not None:
        spec.deadline_in_s = float(deadline_in)
    elif deadline_at is not None:
        spec.deadline_unix_ns = _unix_ns(deadline_at)
    if start_in is not None:
        spec.start_in_s = float(start_in)
    elif start_at is not None:
        spec.start_unix_ns = _unix_ns(start_at)
    if tolerance is not None:
        spec.tolerance_s = float(tolerance)
    if on_miss is not None:
        try:
            spec.on_miss = _MISS[on_miss]
        except KeyError:
            raise ValueError(f"unknown on_miss {on_miss!r}") from None
    if duration_hint is not None:
        spec.duration_hint_s = float(duration_hint)
    if timeout is not None:
        spec.timeout_s = float(timeout)
    if retry is not None:
        spec.retry.CopyFrom(_retry(retry))
    if after is None:
        after_refs: Iterable[TaskRef] = ()
    elif isinstance(after, (str, int)):
        after_refs = (after,)
    else:
        after_refs = after
    for a in after_refs:
        spec.deps.append(pb.Dep(target=_ref(a), cond=pb.EDGE_COND_ON_SUCCESS))
    for d in deps:
        if isinstance(d, Mapping):
            ref, cond = d["ref"], d.get("cond", "on_success")
        else:
            ref, cond = d
        try:
            spec.deps.append(pb.Dep(target=_ref(ref), cond=_CONDS[cond]))
        except KeyError:
            raise ValueError(f"unknown dependency condition {cond!r}") from None
    for name, amount in (resources or {}).items():
        spec.resources[name] = int(amount)
    for k, v in (tags or {}).items():
        spec.tags[k] = str(v)
    return spec


# ---------------------------------------------------------------------------------------------
# Channels


def _target(t: str) -> str:
    for prefix in ("http://", "https://", "grpc://"):
        if t.startswith(prefix):
            return t[len(prefix):].rstrip("/")
    return t


def _channel(
    target: str,
    tls: bool,
    credentials: Optional[grpc.ChannelCredentials],
    options: Optional[Sequence[Tuple[str, Any]]],
) -> grpc.aio.Channel:
    opts = list(options or [])
    if credentials is not None or tls or target.startswith("https://"):
        return grpc.aio.secure_channel(_target(target), credentials or grpc.ssl_channel_credentials(), options=opts)
    return grpc.aio.insecure_channel(_target(target), options=opts)


#: Extra time the client waits beyond a server-side ``Wait`` timeout before giving up itself.
_WAIT_DEADLINE_MARGIN = 5.0


def _metadata(token: Optional[str]) -> Tuple[Tuple[str, str], ...]:
    return (("authorization", f"Bearer {token}"),) if token else ()


# ---------------------------------------------------------------------------------------------
# Client


class Client:
    """Submit and inspect tasks on a cronoq server.

    ``target`` is ``host:port`` (an ``http://``/``https://`` prefix is accepted). Use
    ``tls=True`` for TLS with system roots, or pass ``grpc.ssl_channel_credentials(...)`` as
    ``credentials`` (e.g. for mTLS).

    The client can be created anywhere; its gRPC channel is opened on first use on the running
    event loop (and reopened if used from a different loop later).
    """

    def __init__(
        self,
        target: str,
        *,
        token: Optional[str] = None,
        tls: bool = False,
        credentials: Optional[grpc.ChannelCredentials] = None,
        options: Optional[Sequence[Tuple[str, Any]]] = None,
    ) -> None:
        self.target = target
        self._tls = tls
        self._credentials = credentials
        self._options = options
        self._md = _metadata(token)
        self._channel: Optional[grpc.aio.Channel] = None
        self._channel_loop: Optional[asyncio.AbstractEventLoop] = None
        self._tasks_stub: Any = None
        self._admin_stub: Any = None

    def _ensure_channel(self) -> None:
        loop = asyncio.get_running_loop()
        if self._channel is None or self._channel_loop is not loop:
            self._channel = _channel(self.target, self._tls, self._credentials, self._options)
            self._channel_loop = loop
            self._tasks_stub = pb_grpc.TasksStub(self._channel)
            self._admin_stub = pb_grpc.AdminStub(self._channel)

    @property
    def _tasks(self) -> Any:
        self._ensure_channel()
        return self._tasks_stub

    @property
    def _admin(self) -> Any:
        self._ensure_channel()
        return self._admin_stub

    async def __aenter__(self) -> "Client":
        self._ensure_channel()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        channel, self._channel = self._channel, None
        if channel is not None and self._channel_loop is asyncio.get_running_loop():
            await channel.close()
        self._channel_loop = None

    async def _call(self, fn: Callable[..., Awaitable[Any]], req: Any, timeout: Optional[float] = None) -> Any:
        try:
            return await fn(req, metadata=self._md, timeout=timeout)
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED and timeout is not None:
                raise TimeoutError(f"no response within {timeout}s") from None
            raise ClientError.from_rpc(e) from None

    async def submit(
        self,
        payload: Payload = None,
        *,
        wait: bool = False,
        call_timeout: Optional[float] = None,
        **opts: Any,
    ) -> Submitted:
        """Submit one task (see :func:`task` for options; ``timeout`` there is the task's
        execution timeout). With ``wait=True`` the call returns when the task finished, with
        ``Submitted.result`` set. ``call_timeout`` (seconds) bounds the whole call;
        :class:`TimeoutError` is raised when it expires (the task keeps running on the server)."""
        req = pb.SubmitRequest(task=task(payload, **opts), wait=wait)
        r = await self._call(self._tasks.Submit, req, call_timeout)
        return Submitted(r.id, r.duplicate, Result._from_pb(r.result) if r.HasField("result") else None)

    async def submit_batch(self, tasks: Sequence["pb.TaskSpec"]) -> List[Submitted]:
        """Submit specs built with :func:`task` atomically; they may reference each other's keys."""
        r = await self._call(self._tasks.SubmitBatch, pb.SubmitBatchRequest(tasks=list(tasks)))
        return [Submitted(t.id, t.duplicate) for t in r.tasks]

    async def cancel(self, ref: TaskRef) -> bool:
        r = await self._call(self._tasks.Cancel, _ref(ref))
        return r.cancelled

    async def get(self, ref: TaskRef) -> Optional[TaskInfo]:
        """Current status, or ``None`` if the task is unknown (or no longer retained)."""
        r = await self._call(self._tasks.Get, _ref(ref))
        return TaskInfo(r.id, _status_name(r.status)) if r.found else None

    async def wait(self, ref: TaskRef, timeout: Optional[float] = None) -> Result:
        """Wait for a task to finish. Raises :class:`TimeoutError` after ``timeout`` seconds.

        The server enforces ``timeout``; the client also applies a gRPC deadline slightly
        later, so a half-open connection cannot block forever."""
        req = pb.WaitRequest(task=_ref(ref))
        deadline = None
        if timeout is not None:
            req.timeout_s = float(timeout)
            deadline = float(timeout) + _WAIT_DEADLINE_MARGIN
        try:
            r = await self._tasks.Wait(req, metadata=self._md, timeout=deadline)
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise TimeoutError(f"task {ref!r} still running after {timeout}s") from None
            raise ClientError.from_rpc(e) from None
        return Result._from_pb(r)

    async def watch(self, pools: Optional[Iterable[str]] = None) -> AsyncIterator[Event]:
        """Stream status events (terminal ones, plus all transitions if the server emits them)."""
        call = self._tasks.Watch(pb.WatchRequest(pools=list(pools or [])), metadata=self._md)
        try:
            async for e in call:
                yield Event(
                    id=e.id,
                    pool=e.pool,
                    key=e.key,
                    status=_status_name(e.status),
                    attempt=e.attempt,
                    at=e.at_unix_ns / 1e9,
                    message=e.message,
                )
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                raise ClientError.from_rpc(e) from None
        finally:
            call.cancel()

    async def snapshot(self) -> Snapshot:
        """Windowed quality metrics per pool (requires an admin token)."""
        r = await self._call(self._admin.Snapshot, pb.Empty())
        return Snapshot(
            pools=tuple(
                PoolSnapshot(
                    name=p.name,
                    capacity=p.capacity,
                    busy=p.busy,
                    held=p.held,
                    desired=p.desired,
                    pending_by_class=tuple(p.pending_by_class),
                    reservations=p.reservations,
                    dispatched=p.dispatched,
                    succeeded=p.succeeded,
                    failed=p.failed,
                    priority_adherence=p.priority_adherence,
                    fixed_start_hit_rate=p.fixed_start_hit_rate,
                    utilization=p.utilization,
                    schedule_efficiency=p.schedule_efficiency,
                    wait_p50=p.wait_p50_s,
                    wait_p90=p.wait_p90_s,
                    wait_p99=p.wait_p99_s,
                    cost=p.cost,
                )
                for p in r.pools
            ),
            live_tasks=r.live_tasks,
            running=r.running,
            pending=r.pending,
        )

    async def add_recurring(
        self,
        name: str,
        cron: str,
        payload: Union[Payload, "pb.TaskSpec"] = None,
        *,
        tz: Optional[str] = None,
        overlap: str = "skip",
        allow_overlap: Optional[bool] = None,
        **opts: Any,
    ) -> None:
        """Add a cron schedule (requires an admin token).

        Like the native ``Scheduler.add_recurring``: ``payload`` plus :func:`task` options, or a
        prebuilt spec from :func:`task`. ``overlap`` is ``"skip"`` (default) or ``"allow"``;
        ``allow_overlap`` is accepted as an older spelling."""
        if isinstance(payload, pb.TaskSpec):
            if opts:
                raise TypeError("task options cannot be combined with a prebuilt TaskSpec")
            template = payload
        else:
            template = task(payload, **opts)
        if overlap not in ("skip", "allow"):
            raise ValueError("overlap must be 'skip' or 'allow'")
        allow = (overlap == "allow") if allow_overlap is None else bool(allow_overlap)
        req = pb.RecurringRequest(name=name, cron=cron, tz=tz or "", template=template, allow_overlap=allow)
        await self._call(self._admin.AddRecurring, req)

    async def remove_recurring(self, name: str) -> bool:
        r = await self._call(self._admin.RemoveRecurring, pb.RemoveRecurringRequest(name=name))
        return r.removed


# ---------------------------------------------------------------------------------------------
# Worker


@dataclass
class Lease:
    """One leased attempt handed to a worker handler."""

    id: int
    task_id: int
    attempt: int
    payload: bytes
    content_type: str
    kind: str
    key: str
    tags: Dict[str, str]
    timeout: Optional[float]
    deadline: Optional[float]  # Unix seconds
    #: Set when the server aborts this attempt (timeout, lease loss, cancellation).
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)

    def json(self) -> Any:
        return _decode_json(self.payload)

    def text(self) -> str:
        return self.payload.decode()


Handler = Callable[[Lease], Awaitable[Payload]]

# Errors on which reconnecting is pointless.
_FATAL = {
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.FAILED_PRECONDITION,
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.UNIMPLEMENTED,
}

_STOP = object()


class Worker:
    """Serve a pool: lease tasks from the server and run ``handler`` on them.

    ``handler(lease)`` returns the output (``bytes``, ``str``, JSON-serialisable values or
    ``None``). Raise :class:`cronoq.Retryable` / :class:`cronoq.Permanent` to control retries;
    any other exception is reported as a retryable failure (an output that cannot be
    JSON-encoded is a permanent failure). At most ``slots`` leases run at once. If the stream
    drops, the worker reconnects with exponential backoff (unless ``reconnect=False``); attempts
    in flight at that moment are retried by the server.

    A worker can be created outside a running event loop and run again after :meth:`stop`
    (also on a different loop).

    Note: when the server aborts an attempt (timeout, lease loss, cancellation), the handler's
    task is cancelled and the slot is freed immediately. A handler that swallows
    ``asyncio.CancelledError`` keeps running and may overlap with the retry of the same task.
    """

    def __init__(
        self,
        target: str,
        pool: str,
        handler: Handler,
        *,
        slots: int = 1,
        token: Optional[str] = None,
        name: str = "",
        tls: bool = False,
        credentials: Optional[grpc.ChannelCredentials] = None,
        reconnect: bool = True,
        backoff: Tuple[float, float] = (0.1, 5.0),
        grace: float = 10.0,
        options: Optional[Sequence[Tuple[str, Any]]] = None,
    ) -> None:
        if slots < 1:
            raise ValueError("slots must be >= 1")
        self.pool = pool
        self.slots = slots
        self.name = name
        self.handler = handler
        self.reconnect = reconnect
        self.backoff = backoff
        self.grace = grace
        self._target = target
        self._tls = tls
        self._credentials = credentials
        self._options = options
        self._md = _metadata(token)
        # Created lazily on the running loop (asyncio primitives bind to a loop on Python 3.9).
        self._events_loop: Optional[asyncio.AbstractEventLoop] = None
        self._stopping_ev: Optional[asyncio.Event] = None
        self._registered_ev: Optional[asyncio.Event] = None
        self._closed_ev: Optional[asyncio.Event] = None
        self._stop_task: Optional["asyncio.Future[None]"] = None
        self._draining = False
        self._finished = False
        self._outbox: Optional[asyncio.Queue] = None
        self._running: Dict[int, Tuple[asyncio.Task, Lease]] = {}
        self._call: Any = None
        self.worker_id: Optional[int] = None
        self.lease_ttl: Optional[float] = None
        #: Number of (re)connections made, for monitoring.
        self.connections = 0

    def _ensure_events(self) -> None:
        loop = asyncio.get_running_loop()
        if self._events_loop is not loop:
            self._events_loop = loop
            self._stopping_ev = asyncio.Event()
            self._registered_ev = asyncio.Event()
            self._closed_ev = asyncio.Event()
            self._stop_task = None

    @property
    def _stopping(self) -> asyncio.Event:
        self._ensure_events()
        assert self._stopping_ev is not None
        return self._stopping_ev

    @property
    def _registered(self) -> asyncio.Event:
        self._ensure_events()
        assert self._registered_ev is not None
        return self._registered_ev

    @property
    def _closed(self) -> asyncio.Event:
        self._ensure_events()
        assert self._closed_ev is not None
        return self._closed_ev

    @property
    def running(self) -> int:
        return len(self._running)

    async def wait_registered(self, timeout: Optional[float] = None) -> None:
        """Wait until the worker is registered with the server."""
        await asyncio.wait_for(self._registered.wait(), timeout)

    async def run(self) -> None:
        """Serve until :meth:`stop` is called (or a non-retryable error occurs)."""
        self._ensure_events()
        if self._finished:
            # Running again after a previous run ended: start from a clean state.
            self._finished = False
            self._draining = False
            self._stop_task = None
            self._stopping.clear()
            self._registered.clear()
            self._closed.clear()
        try:
            await self._serve()
        finally:
            self._finished = True

    async def _serve(self) -> None:
        delay = self.backoff[0]
        while not self._stopping.is_set():
            started = time.monotonic()
            try:
                await self._session()
            except grpc.aio.AioRpcError as e:
                if self._stopping.is_set():
                    break
                if e.code() in _FATAL or not self.reconnect:
                    raise ClientError.from_rpc(e) from None
                log.warning("worker stream failed (%s); reconnecting", e.code().name)
            finally:
                self._registered.clear()
                self._abandon_running()
            if self._stopping.is_set() or self._draining:
                break  # never reconnect while stopping
            if not self.reconnect:
                raise ClientError("unavailable", "worker stream ended")
            if time.monotonic() - started > 10 * self.backoff[1]:
                delay = self.backoff[0]  # the last session was healthy
            try:
                await asyncio.wait_for(self._stopping.wait(), delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, self.backoff[1])

    async def stop(self, grace: Optional[float] = None) -> None:
        """Stop taking leases, let running ones finish for up to ``grace`` seconds, then
        cancel the rest and disconnect. Concurrent calls wait for the same shutdown."""
        self._ensure_events()
        if self._stop_task is None:
            self._stop_task = asyncio.ensure_future(self._stop(self.grace if grace is None else grace))
        await asyncio.shield(self._stop_task)

    async def _stop(self, grace: float) -> None:
        self._draining = True
        if self._outbox is not None and self._registered.is_set():
            self._outbox.put_nowait(pb.WorkerMessage(set_slots=pb.SetSlots(slots=0)))
        # Wait for running leases, including any that arrived just before the server saw
        # SetSlots(0).
        deadline = time.monotonic() + grace
        while self._running:
            left = deadline - time.monotonic()
            if left <= 0:
                break
            await asyncio.wait([t for t, _ in self._running.values()], timeout=min(left, 0.1))
        self._stopping.set()
        for t, lease in list(self._running.values()):
            lease.cancelled.set()
            t.cancel()
        if self._outbox is not None:
            # Half-close after the queued results; the server then ends the stream.
            self._outbox.put_nowait(_STOP)
        if self._call is not None:
            try:
                await asyncio.wait_for(self._closed.wait(), 5.0)
            except asyncio.TimeoutError:
                if self._call is not None:
                    self._call.cancel()

    def _abandon_running(self) -> None:
        for t, lease in list(self._running.values()):
            lease.cancelled.set()
            t.cancel()
        self._running.clear()

    async def _session(self) -> None:
        outbox: asyncio.Queue = asyncio.Queue()
        self._outbox = outbox
        self._closed.clear()
        outbox.put_nowait(
            pb.WorkerMessage(register=pb.Register(pool=self.pool, slots=self.slots, worker_name=self.name))
        )

        async def requests() -> AsyncIterator["pb.WorkerMessage"]:
            while True:
                msg = await outbox.get()
                if msg is _STOP:
                    return
                yield msg

        async with _channel(self._target, self._tls, self._credentials, self._options) as channel:
            stub = pb_grpc.WorkersStub(channel)
            call = stub.Work(requests(), metadata=self._md)
            self._call = call
            self.connections += 1
            try:
                async for msg in call:
                    kind = msg.WhichOneof("msg")
                    if kind == "registered":
                        self.worker_id = msg.registered.worker_id
                        self.lease_ttl = (
                            msg.registered.lease_ttl_s if msg.registered.HasField("lease_ttl_s") else None
                        )
                        self._registered.set()
                    elif kind == "lease":
                        self._start(msg.lease, outbox)
                    elif kind == "abort":
                        entry = self._running.pop(msg.abort.lease, None)
                        if entry is not None:
                            entry[1].cancelled.set()
                            entry[0].cancel()
            except asyncio.CancelledError:
                if not self._stopping.is_set():
                    raise
            finally:
                self._call = None
                self._closed.set()
                outbox.put_nowait(_STOP)

    def _start(self, m: "pb.Lease", outbox: asyncio.Queue) -> None:
        lease = Lease(
            id=m.lease,
            task_id=m.task_id,
            attempt=m.attempt,
            payload=m.payload,
            content_type=m.content_type,
            kind=m.kind,
            key=m.key,
            tags=dict(m.tags),
            timeout=m.timeout_s if m.HasField("timeout_s") else None,
            deadline=m.deadline_unix_ns / 1e9 if m.HasField("deadline_unix_ns") else None,
        )
        task = asyncio.get_running_loop().create_task(self._run_lease(lease, outbox))
        self._running[lease.id] = (task, lease)

    async def _heartbeat(self, lease: Lease, outbox: asyncio.Queue) -> None:
        if not self.lease_ttl:
            return
        period = max(self.lease_ttl / 3, 0.01)
        while True:
            await asyncio.sleep(period)
            outbox.put_nowait(pb.WorkerMessage(heartbeat=pb.Heartbeat(lease=lease.id)))

    async def _run_lease(self, lease: Lease, outbox: asyncio.Queue) -> None:
        beat = asyncio.get_running_loop().create_task(self._heartbeat(lease, outbox))
        msg: Optional[pb.WorkerMessage] = None
        try:
            out = await self.handler(lease)
            try:
                data, _ = _encode(out, None)
            except (TypeError, ValueError) as e:
                raise Permanent(f"handler output is not JSON-serialisable: {e}") from None
            msg = pb.WorkerMessage(complete=pb.Complete(lease=lease.id, output=data))
        except asyncio.CancelledError:
            current = asyncio.current_task()
            cancelling = getattr(current, "cancelling", None)
            external = (
                lease.cancelled.is_set()
                or self._stopping.is_set()
                or (cancelling is not None and cancelling() > 0)
            )
            if external:
                return  # aborted by the server or the worker is stopping
            # The handler itself raised CancelledError (e.g. awaited a future that something
            # else cancelled): report it, or the server would wait for the lease to expire.
            msg = pb.WorkerMessage(
                fail=pb.Fail(lease=lease.id, message="CancelledError: raised by the handler", permanent=False)
            )
        except Permanent as e:
            msg = pb.WorkerMessage(fail=pb.Fail(lease=lease.id, message=str(e) or "permanent failure", permanent=True))
        except Retryable as e:
            fail = pb.Fail(lease=lease.id, message=str(e) or "retryable failure", permanent=False)
            if e.retry_after is not None:
                fail.retry_after_s = float(e.retry_after)
            msg = pb.WorkerMessage(fail=fail)
        except Exception as e:  # noqa: BLE001 - any handler error is a retryable failure
            log.debug("handler failed for task %s", lease.task_id, exc_info=True)
            msg = pb.WorkerMessage(fail=pb.Fail(lease=lease.id, message=f"{type(e).__name__}: {e}", permanent=False))
        finally:
            beat.cancel()
            self._running.pop(lease.id, None)
            if msg is not None:
                outbox.put_nowait(msg)
