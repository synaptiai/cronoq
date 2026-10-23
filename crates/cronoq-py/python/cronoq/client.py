"""Client and remote worker for a ``cronoq-server`` over gRPC.

Requires the ``client`` extra: ``pip install "cronoq[client]"``.

Speaks worker protocol version 2 (see ``cronoq.v1`` in ``cronoq-proto``).

Submitting work::

    from cronoq.client import Client

    async with Client("localhost:7070", token=os.environ["CRONOQ_CLIENT_TOKEN"]) as c:
        sub = await c.submit({"prompt": "hi"}, pool="llm", priority="high", wait=True)
        print(sub.result.json())

Serving a pool::

    from cronoq.client import Worker

    async def handle(lease):
        return {"echo": lease.json()}

    await Worker("localhost:7070", "llm", handle, slots=8,
                 token=os.environ["CRONOQ_WORKER_TOKEN"]).run()
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import time
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    cast,
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
    "CancelTask",
    "Client",
    "ClientError",
    "Event",
    "Lease",
    "PROTOCOL_VERSION",
    "PoolSnapshot",
    "RecurringInfo",
    "Reject",
    "Result",
    "Snapshot",
    "Submitted",
    "TaskInfo",
    "TaskPage",
    "Totals",
    "Worker",
    "WorkerGroup",
    "WorkerInfo",
    "task",
]

#: Worker protocol version this client speaks.
PROTOCOL_VERSION = 3

log = logging.getLogger("cronoq.client")

Payload = bytes | bytearray | memoryview | str | dict | list | int | float | bool | None
TaskRef = int | str


def _is_deadline(e: grpc.aio.AioRpcError) -> bool:
    """Whether an RPC failed because a deadline expired. grpcio reports a client-side deadline
    as ``DEADLINE_EXCEEDED`` or, in some versions and platforms, as ``CANCELLED`` with the
    details "Timeout expired"."""
    code = e.code()
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return True
    return code == grpc.StatusCode.CANCELLED and "timeout expired" in (e.details() or "").lower()


class ClientError(CronoqError):
    """A gRPC call failed. ``code`` is the lower-case gRPC status name, e.g.
    ``unauthenticated``, ``permission_denied``, ``not_found`` (unknown task, pool or
    dependency), ``resource_exhausted`` (queue full, retry later), ``failed_precondition``
    (reservation conflict), ``invalid_argument``, ``unavailable`` (not the leader or shutting
    down: retry, e.g. against the new leader)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.code, self.message))

    @property
    def retryable(self) -> bool:
        """True for errors where retrying the same call later can succeed."""
        return self.code in ("unavailable", "resource_exhausted")

    @classmethod
    def from_rpc(cls, e: grpc.RpcError) -> ClientError:
        code = e.code() if hasattr(e, "code") else None
        details = e.details() if hasattr(e, "details") else str(e)
        return cls(code.name.lower() if code is not None else "unknown", details or "")


class Reject(CronoqError):
    """Raise from a :class:`Worker` handler to give the lease back without running it (sent as
    ``Nack``). The server offers it, within the same attempt, to another worker of the pool that
    has a free slot; if there is none, the attempt fails as retryable and counts against the
    task's retry budget."""

    def __init__(self, reason: str = "") -> None:
        super().__init__(reason)
        self.reason = reason

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.reason,))


class CancelTask(CronoqError):
    """Raise from a :class:`Worker` handler to end the task as ``cancelled`` (no retry)."""


def _status_name(value: int) -> str:
    name = pb.TaskStatus.Name(value)
    return name[len("TASK_STATUS_"):].lower() if name.startswith("TASK_STATUS_") else name.lower()


# ---------------------------------------------------------------------------------------------
# Results


def _decode_json(data: bytes | None) -> Any:
    if data is None:
        raise ValueError("no output")
    if not data:
        return None  # a handler that returned None
    return json.loads(data)


@dataclass(frozen=True)
class Result:
    """Outcome of a finished task. ``status`` is always terminal (``succeeded``, ``failed``,
    ``dead_lettered``, ``cancelled``, ``skipped``, ``missed`` or ``shed``).
    ``output_available`` is False for a succeeded task whose output is no longer retained."""

    id: int
    status: str
    output: bytes | None
    message: str = ""
    output_available: bool = False

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
    def _from_pb(cls, r: pb.TaskResult) -> Result:
        return cls(
            id=r.id,
            status=_status_name(r.status),
            output=r.output if r.HasField("output") else None,
            message=r.message,
            output_available=r.output_available,
        )


@dataclass(frozen=True)
class Submitted:
    id: int
    duplicate: bool
    result: Result | None = None


def _opt_secs(msg: Any, field_name: str) -> float | None:
    return getattr(msg, field_name) / 1e9 if msg.HasField(field_name) else None


@dataclass(frozen=True)
class TaskInfo:
    """Task details from :meth:`Client.get`. Fields other than ``id`` and ``status`` are best
    effort (known for tasks the serving leader submitted or observed, and for persisted
    tasks). Times are Unix seconds."""

    id: int
    status: str
    pool: str = ""
    key: str = ""
    kind: str = ""
    attempt: int = 0
    #: 0 if unknown.
    max_attempts: int = 0
    created_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    #: Last error or status message.
    message: str = ""
    output_available: bool = False
    tenant: str = ""
    #: ``"critical"``, ``"high"``, ``"normal"`` or ``"low"``; ``None`` if unknown.
    priority: str | None = None

    @property
    def done(self) -> bool:
        return self.status in _TERMINAL

    @classmethod
    def _from_pb(cls, r: pb.TaskInfo) -> TaskInfo:
        return cls(
            id=r.id,
            status=_status_name(r.status),
            pool=r.pool,
            key=r.key,
            kind=r.kind,
            attempt=r.attempt,
            max_attempts=r.max_attempts,
            created_at=_opt_secs(r, "created_unix_ns"),
            started_at=_opt_secs(r, "started_unix_ns"),
            finished_at=_opt_secs(r, "finished_unix_ns"),
            message=r.message,
            output_available=r.output_available,
            tenant=r.tenant,
            priority=_priority_name(r.priority),
        )


@dataclass(frozen=True)
class TaskPage:
    """One page of :meth:`Client.list_tasks`, in id order."""

    tasks: tuple[TaskInfo, ...]
    #: Pass as ``after`` for the next page; ``None`` when this page is the last.
    next_after: int | None
    #: With shards: shards that could not be reached, whose tasks are missing.
    unavailable_shards: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkerInfo:
    """A connected remote worker, from :meth:`Client.list_workers`."""

    id: int
    name: str
    pool: str
    labels: dict[str, str]
    slots: int
    busy: int
    #: Unix seconds.
    connected_at: float


@dataclass(frozen=True)
class RecurringInfo:
    """A recurring schedule, from :meth:`Client.list_recurring`."""

    name: str
    cron: str
    #: Empty for UTC.
    tz: str
    pool: str
    kind: str
    overlap: str
    #: Unix seconds; ``None`` when the schedule has no further occurrence.
    next_at: float | None
    #: Task id of the latest occurrence.
    last_task_id: int | None


def _status_value(name: str) -> int:
    try:
        value = pb.TaskStatus.Value("TASK_STATUS_" + name.upper())
    except ValueError:
        value = pb.TASK_STATUS_UNSPECIFIED
    if value == pb.TASK_STATUS_UNSPECIFIED:
        raise ValueError(f"unknown task status {name!r}")
    return value


_TERMINAL = frozenset({"succeeded", "failed", "dead_lettered", "cancelled", "skipped", "missed", "shed"})


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
class WorkerGroup:
    """Workers of a pool that share one label set."""

    labels: Mapping[str, str]
    capacity: int
    busy: int


@dataclass(frozen=True)
class PoolSnapshot:
    name: str
    capacity: int
    busy: int
    held: int
    desired: int
    pending_by_class: tuple[int, ...]
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
    workers_connected: int = 0
    groups: tuple[WorkerGroup, ...] = ()

    @property
    def pending(self) -> int:
        return sum(self.pending_by_class)


@dataclass(frozen=True)
class Totals:
    """Counters since the leader started."""

    submitted: int = 0
    dispatched: int = 0
    succeeded: int = 0
    failed: int = 0
    dead_lettered: int = 0
    cancelled: int = 0
    skipped: int = 0
    missed: int = 0
    shed: int = 0
    retries: int = 0
    timeouts: int = 0
    lease_expiries: int = 0
    stale_completions: int = 0


@dataclass(frozen=True)
class Snapshot:
    pools: tuple[PoolSnapshot, ...]
    live_tasks: int
    running: int
    pending: int
    totals: Totals = field(default_factory=Totals)
    workers_connected: int = 0
    #: With shards: shards that could not be reached, whose pools are missing.
    unavailable_shards: tuple[str, ...] = ()

    def pool(self, name: str) -> PoolSnapshot | None:
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


def _encode(payload: Payload, content_type: str | None) -> tuple[bytes, str]:
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload), content_type or "application/octet-stream"
    if isinstance(payload, str):
        return payload.encode(), content_type or "text/plain; charset=utf-8"
    if payload is None:
        return b"", content_type or ""
    return json.dumps(payload).encode(), content_type or "application/json"


def _ref(r: TaskRef) -> pb.TaskRef:
    if isinstance(r, bool):
        raise TypeError("task reference must be an id (int) or key (str)")
    if isinstance(r, int):
        return pb.TaskRef(id=r)
    if isinstance(r, str):
        return pb.TaskRef(key=r)
    raise TypeError("task reference must be an id (int) or key (str)")


def _unix_ns(t: datetime | float | int) -> int:
    if isinstance(t, datetime):
        return int(t.timestamp() * 1e9)
    return int(float(t) * 1e9)


def _retry(r: Any) -> pb.RetryPolicy:
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
    pool: str | None = None,
    kind: str | None = None,
    priority: Any = None,
    key: str | None = None,
    deadline_in: float | None = None,
    deadline_at: datetime | float | None = None,
    start_in: float | None = None,
    start_at: datetime | float | None = None,
    tolerance: float | None = None,
    on_miss: str | None = None,
    duration_hint: float | None = None,
    timeout: float | None = None,
    retry: Any = None,
    after: TaskRef | Iterable[TaskRef] | None = None,
    deps: Iterable[tuple[TaskRef, str] | Mapping[str, Any]] = (),
    resources: Mapping[str, int] | None = None,
    tags: Mapping[str, str] | None = None,
    labels: Mapping[str, str] | None = None,
    tenant: str | None = None,
    content_type: str | None = None,
) -> pb.TaskSpec:
    """Build a wire task spec (for :meth:`Client.submit_batch` and :meth:`Client.add_recurring`).

    Times are seconds: ``*_in`` relative to now, ``*_at`` absolute (Unix seconds or a
    timezone-aware ``datetime``); give at most one of each pair. ``after`` is a key, an id or a
    list of them (each must succeed first). ``deps`` entries are ``(ref, cond)`` tuples or
    ``{"ref": ..., "cond": ...}`` with ``cond`` in ``success``/``complete``/``failure`` (or the
    ``on_``-prefixed spellings). ``labels`` are worker labels the task requires.
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
        # Protobuf enum fields accept their integer values; the generated stubs only list names.
        priority=cast(Any, _priority(priority)),
        key=key or "",
        tenant=tenant or "",
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
    for k, v in (labels or {}).items():
        spec.labels[k] = str(v)
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
    credentials: grpc.ChannelCredentials | None,
    options: Sequence[tuple[str, Any]] | None,
) -> grpc.aio.Channel:
    opts = list(options or [])
    if credentials is not None or tls or target.startswith("https://"):
        return grpc.aio.secure_channel(_target(target), credentials or grpc.ssl_channel_credentials(), options=opts)
    return grpc.aio.insecure_channel(_target(target), options=opts)


#: Extra time the client waits beyond a server-side ``Wait`` timeout before giving up itself.
_WAIT_DEADLINE_MARGIN = 5.0

#: Room left in a message for everything but the output bytes.
_MESSAGE_OVERHEAD = 1024


def _priority_name(value: int) -> str | None:
    if value == pb.PRIORITY_UNSPECIFIED:
        return None
    return pb.Priority.Name(value)[len("PRIORITY_"):].lower()


def _metadata(token: str | None) -> tuple[tuple[str, str], ...]:
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
        token: str | None = None,
        tls: bool = False,
        credentials: grpc.ChannelCredentials | None = None,
        options: Sequence[tuple[str, Any]] | None = None,
    ) -> None:
        self.target = target
        self._tls = tls
        self._credentials = credentials
        self._options = options
        self._md = _metadata(token)
        self._channel: grpc.aio.Channel | None = None
        self._channel_loop: asyncio.AbstractEventLoop | None = None
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

    async def __aenter__(self) -> Client:
        self._ensure_channel()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        channel, self._channel = self._channel, None
        if channel is not None and self._channel_loop is asyncio.get_running_loop():
            await channel.close()
        self._channel_loop = None

    async def _call(self, fn: Callable[..., Awaitable[Any]], req: Any, timeout: float | None = None) -> Any:
        try:
            return await fn(req, metadata=self._md, timeout=timeout)
        except grpc.aio.AioRpcError as e:
            if _is_deadline(e):
                # Either our own deadline, or the server's wait limit (task still running).
                raise TimeoutError(e.details() or f"no response within {timeout}s") from None
            raise ClientError.from_rpc(e) from None

    async def submit(
        self,
        payload: Payload = None,
        *,
        wait: bool = False,
        wait_timeout: float | None = None,
        call_timeout: float | None = None,
        **opts: Any,
    ) -> Submitted:
        """Submit one task (see :func:`task` for options; ``timeout`` there is the task's
        execution timeout).

        With ``wait=True`` the call returns when the task finished, with ``Submitted.result``
        set. ``wait_timeout`` bounds the wait on the server (which also caps it at its
        ``limits.max_wait_s``); ``call_timeout`` bounds the whole call on the client. Either
        expiring raises :class:`TimeoutError` (the task stays submitted: ``wait`` on it
        later). :class:`ClientError` ``unavailable`` means the server shut down or lost
        leadership before the task finished."""
        req = pb.SubmitRequest(task=task(payload, **opts), wait=wait)
        if wait_timeout is not None:
            if not wait:
                raise ValueError("wait_timeout requires wait=True")
            req.wait_timeout_s = float(wait_timeout)
            if call_timeout is None:
                call_timeout = float(wait_timeout) + _WAIT_DEADLINE_MARGIN
        r = await self._call(self._tasks.Submit, req, call_timeout)
        return Submitted(r.id, r.duplicate, Result._from_pb(r.result) if r.HasField("result") else None)

    async def submit_batch(self, tasks: Sequence[pb.TaskSpec]) -> list[Submitted]:
        """Submit specs built with :func:`task` atomically; they may reference each other's keys."""
        r = await self._call(self._tasks.SubmitBatch, pb.SubmitBatchRequest(tasks=list(tasks)))
        return [Submitted(t.id, t.duplicate) for t in r.tasks]

    async def cancel(self, ref: TaskRef) -> bool:
        """Cancel a task by id or key; ``False`` if it already finished."""
        r = await self._call(self._tasks.Cancel, pb.CancelRequest(task=_ref(ref)))
        return r.cancelled

    async def get(self, ref: TaskRef) -> TaskInfo:
        """Task details by id or key. Raises :class:`ClientError` ``not_found`` if the task is
        unknown (or no longer retained)."""
        r = await self._call(self._tasks.Get, pb.GetRequest(task=_ref(ref)))
        return TaskInfo._from_pb(r)

    async def list_tasks(
        self,
        *,
        pools: Sequence[str] = (),
        statuses: Sequence[str] = (),
        after: int = 0,
        limit: int = 0,
    ) -> TaskPage:
        """One page of tasks in id order: live and finished, of ``pools`` (default: all the
        token may see) with one of ``statuses`` (default: any), with ids above ``after``.
        ``limit`` defaults to 100 and is capped by the server's ``limits.max_list``. Finished
        tasks come from the server's store, or from its bounded cache without one."""
        if isinstance(pools, str) or isinstance(statuses, str):
            raise TypeError("pools and statuses are sequences of names")
        req = pb.ListTasksRequest(
            pools=list(pools),
            statuses=cast(Any, [_status_value(s) for s in statuses]),
            after=after,
            limit=limit,
        )
        r = await self._call(self._tasks.ListTasks, req)
        return TaskPage(
            tasks=tuple(TaskInfo._from_pb(t) for t in r.tasks),
            next_after=r.next_after or None,
            unavailable_shards=tuple(r.unavailable_shards),
        )

    async def iter_tasks(
        self,
        *,
        pools: Sequence[str] = (),
        statuses: Sequence[str] = (),
        page_size: int = 0,
    ) -> AsyncIterator[TaskInfo]:
        """Every task matching the filters, in id order, fetched one page at a time."""
        after = 0
        while True:
            page = await self.list_tasks(pools=pools, statuses=statuses, after=after, limit=page_size)
            for t in page.tasks:
                yield t
            if page.next_after is None:
                return
            after = page.next_after

    async def wait(self, ref: TaskRef, timeout: float | None = None) -> Result:
        """Wait for a task to finish and return its :class:`Result`.

        Raises :class:`TimeoutError` if it is still running after ``timeout`` seconds (or the
        server's ``limits.max_wait_s``), and :class:`ClientError` ``unavailable`` if the server
        shut down or lost leadership first (wait again, against the new leader). The client
        also applies a gRPC deadline slightly after ``timeout``, so a half-open connection
        cannot block forever."""
        req = pb.WaitRequest(task=_ref(ref))
        deadline = None
        if timeout is not None:
            req.timeout_s = float(timeout)
            deadline = float(timeout) + _WAIT_DEADLINE_MARGIN
        try:
            r = await self._tasks.Wait(req, metadata=self._md, timeout=deadline)
        except grpc.aio.AioRpcError as e:
            if _is_deadline(e):
                raise TimeoutError(f"task {ref!r} still running after {timeout}s") from None
            raise ClientError.from_rpc(e) from None
        return Result._from_pb(r)

    async def watch(
        self,
        pools: Iterable[str] | None = None,
        *,
        ids: Iterable[int] | None = None,
        keys: Iterable[str] | None = None,
        all_transitions: bool = False,
    ) -> AsyncIterator[Event]:
        """Stream status events, optionally filtered by pool, task id or key.

        Only terminal statuses are sent unless ``all_transitions=True`` (then also blocked,
        ready, running, retrying, ...). The stream ends with :class:`ClientError`
        ``unavailable`` when the server shuts down or loses leadership, and ``data_loss`` if
        this consumer falls too far behind."""
        req = pb.WatchRequest(
            pools=list(pools or []),
            ids=[int(i) for i in ids or []],
            keys=list(keys or []),
            all_transitions=all_transitions,
        )
        call = self._tasks.Watch(req, metadata=self._md)
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
        """Windowed quality metrics per pool, totals and connected workers (requires a
        ``metrics`` or ``admin`` token)."""
        r = await self._call(self._admin.Snapshot, pb.Empty())
        t = r.totals
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
                    workers_connected=p.workers_connected,
                    groups=tuple(
                        WorkerGroup(labels=dict(g.labels), capacity=g.capacity, busy=g.busy)
                        for g in p.groups
                    ),
                )
                for p in r.pools
            ),
            live_tasks=r.live_tasks,
            running=r.running,
            pending=r.pending,
            totals=Totals(
                submitted=t.submitted,
                dispatched=t.dispatched,
                succeeded=t.succeeded,
                failed=t.failed,
                dead_lettered=t.dead_lettered,
                cancelled=t.cancelled,
                skipped=t.skipped,
                missed=t.missed,
                shed=t.shed,
                retries=t.retries,
                timeouts=t.timeouts,
                lease_expiries=t.lease_expiries,
                stale_completions=t.stale_completions,
            ),
            workers_connected=r.workers_connected,
            unavailable_shards=tuple(r.unavailable_shards),
        )

    async def add_recurring(
        self,
        name: str,
        cron: str,
        payload: Payload | pb.TaskSpec = None,
        *,
        tz: str | None = None,
        overlap: str = "skip",
        **opts: Any,
    ) -> None:
        """Add a cron schedule (requires an admin token).

        Like the native ``Scheduler.add_recurring``: ``payload`` plus :func:`task` options, or a
        prebuilt spec from :func:`task`. ``overlap`` is ``"skip"`` (default), ``"allow"``,
        ``"queue"`` or ``"queue_on_success"``. ``deadline_in`` is relative to each
        occurrence's start; ``key`` is a prefix (each occurrence's key is
        ``"<key>@<start in UTC>"``)."""
        if isinstance(payload, pb.TaskSpec):
            if opts:
                raise TypeError("task options cannot be combined with a prebuilt TaskSpec")
            template = payload
        else:
            template = task(payload, **opts)
        policies = {
            "skip": pb.OVERLAP_SKIP,
            "allow": pb.OVERLAP_ALLOW,
            "queue": pb.OVERLAP_QUEUE,
            "queue_on_success": pb.OVERLAP_QUEUE_ON_SUCCESS,
        }
        if overlap not in policies:
            raise ValueError(f"overlap must be one of {', '.join(policies)}")
        req = pb.RecurringRequest(name=name, cron=cron, tz=tz or "", template=template, overlap=policies[overlap])
        await self._call(self._admin.AddRecurring, req)

    async def remove_recurring(self, name: str) -> bool:
        r = await self._call(self._admin.RemoveRecurring, pb.RemoveRecurringRequest(name=name))
        return r.removed

    async def list_workers(self) -> list[WorkerInfo]:
        """Connected remote workers of the pools the token may see (requires a ``metrics`` or
        ``admin`` token)."""
        r = await self._call(self._admin.ListWorkers, pb.Empty())
        return [
            WorkerInfo(
                id=w.id,
                name=w.name,
                pool=w.pool,
                labels=dict(w.labels),
                slots=w.slots,
                busy=w.busy,
                connected_at=w.connected_unix_ns / 1e9,
            )
            for w in r.workers
        ]

    async def list_recurring(self) -> list[RecurringInfo]:
        """Recurring schedules of the pools the token may see, by name (requires a ``metrics``
        or ``admin`` token)."""
        r = await self._call(self._admin.ListRecurring, pb.Empty())
        return [
            RecurringInfo(
                name=s.name,
                cron=s.cron,
                tz=s.tz,
                pool=s.pool,
                kind=s.kind,
                overlap=pb.Overlap.Name(s.overlap)[len("OVERLAP_"):].lower()
                if s.overlap != pb.OVERLAP_UNSPECIFIED
                else "skip",
                next_at=_opt_secs(s, "next_unix_ns"),
                last_task_id=s.last_task_id if s.HasField("last_task_id") else None,
            )
            for s in r.schedules
        ]


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
    tags: dict[str, str]
    timeout: float | None
    deadline: float | None  # Unix seconds
    pool: str = ""
    #: ``critical``/``high``/``normal``/``low``, or ``None`` if the server did not say.
    priority: str | None = None
    #: Heartbeat at least this often (seconds); sent automatically by the worker.
    lease_ttl: float | None = None
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

#: Redirects to other shards a worker follows in a row before it gives up.
MAX_REDIRECTS = 3

#: Channel options that name the original server, dropped when following a redirect.
_SERVER_NAME_OPTIONS = ("grpc.ssl_target_name_override", "grpc.default_authority")


def _is_loopback(target: str) -> bool:
    rest = target.split("://", 1)[-1].split("/", 1)[0]
    if rest.startswith("["):
        host = rest[1:].split("]", 1)[0]
    else:
        host = rest.rsplit(":", 1)[0]
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _redirect(
    e: grpc.aio.AioRpcError, secure: bool, has_token: bool, insecure_http: bool
) -> str | None:
    """The shard endpoint a refusal sends the worker to, or ``None`` if it is not a redirect.
    Raises :class:`ClientError` for an endpoint that may not be followed: plain http after TLS,
    or a token over plain http to another host without ``insecure_redirect_http``."""
    if e.code() != grpc.StatusCode.FAILED_PRECONDITION:
        return None
    target = None
    for md in (e.trailing_metadata(), e.initial_metadata()):
        for k, v in md or ():
            if k == "cronoq-shard-endpoint" and isinstance(v, str):
                target = v
    if target is None:
        return None
    if not target.startswith(("http://", "https://")):
        raise ClientError(
            "failed_precondition",
            f"the server redirected the worker to {target!r}, which is not an http(s) endpoint",
        )
    plain = target.startswith("http://")
    if plain and secure:
        raise ClientError(
            "failed_precondition",
            f"the server redirected the worker from TLS to plain http ({target!r}); refusing",
        )
    if plain and has_token and not _is_loopback(target):
        if not insecure_http:
            raise ClientError(
                "failed_precondition",
                f"the server redirected the worker to {target!r}, which would send its token over plain http "
                "to another host; refusing (insecure_redirect_http=True allows it)",
            )
        log.warning("following a redirect that sends the worker token over plain http to %s", target)
    return target


class Worker:
    """Serve a pool: lease tasks from the server and run ``handler`` on them.

    ``handler(lease)`` returns the output (``bytes``, ``str``, JSON-serialisable values or
    ``None``). In the handler:

    - raise :class:`cronoq.Retryable` / :class:`cronoq.Permanent` to control retries; any
      other exception is reported as a retryable failure (an output that cannot be JSON-encoded,
      or that exceeds the server's message limit, is a permanent failure);
    - raise :class:`Reject` to hand the lease back unrun (the server offers it to another
      worker with a free slot, or else fails the attempt as retryable);
    - raise :class:`CancelTask` to end the task as ``cancelled``.

    At most ``slots`` leases run at once; the server may grant fewer (``granted_slots``), and
    leases beyond the grant are handed back. If the stream drops, or the server announces it is
    shutting down or no longer the leader, the worker reconnects with exponential backoff
    (unless ``reconnect=False``); attempts in flight at that moment are retried by the server.

    If another shard serves the pool, the server names that shard's endpoint and the worker
    connects there (up to :data:`MAX_REDIRECTS` times in a row), keeping its TLS credentials.
    It never follows a redirect from TLS to plain http, and sends its token over plain http
    only to loopback unless ``insecure_redirect_http=True``. When that shard is unreachable,
    the worker asks ``target`` again.

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
        token: str | None = None,
        name: str = "",
        tls: bool = False,
        credentials: grpc.ChannelCredentials | None = None,
        reconnect: bool = True,
        backoff: tuple[float, float] = (0.1, 5.0),
        grace: float = 10.0,
        options: Sequence[tuple[str, Any]] | None = None,
        labels: Mapping[str, str] | None = None,
        insecure_redirect_http: bool = False,
    ) -> None:
        if slots < 1:
            raise ValueError("slots must be >= 1")
        self.pool = pool
        self.slots = slots
        self.labels = {str(k): str(v) for k, v in (labels or {}).items()}
        self.name = name
        self.handler = handler
        self.reconnect = reconnect
        self.backoff = backoff
        self.grace = grace
        self._target = target
        #: The endpoint connected to: ``target``, or the shard it redirected the worker to.
        self._current = target
        self._insecure_redirect_http = insecure_redirect_http
        self._tls = tls
        self._credentials = credentials
        self._options = options
        self._md = _metadata(token)
        # Created on the loop that runs the worker, so a Worker can run again on a new loop.
        self._events_loop: asyncio.AbstractEventLoop | None = None
        self._stopping_ev: asyncio.Event | None = None
        self._registered_ev: asyncio.Event | None = None
        self._closed_ev: asyncio.Event | None = None
        self._stop_task: asyncio.Future[None] | None = None
        self._draining = False
        self._finished = False
        self._outbox: asyncio.Queue | None = None
        self._running: dict[int, tuple[asyncio.Task, Lease]] = {}
        self._call: Any = None
        self.worker_id: int | None = None
        self.lease_ttl: float | None = None
        #: Slots the server granted (may be lower than requested), once registered.
        self.granted_slots: int | None = None
        #: Worker protocol version the server speaks, once registered.
        self.server_protocol: int | None = None
        #: Largest message the server accepts (outputs must fit), once registered.
        self.max_message_bytes: int | None = None
        #: Reason given by the server in its last ``Shutdown`` message, if any.
        self.last_shutdown: str | None = None
        self._reconnect_now = False
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

    async def wait_registered(self, timeout: float | None = None) -> None:
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

    def _secure(self, target: str) -> bool:
        return self._tls or self._credentials is not None or target.startswith("https://")

    async def _serve(self) -> None:
        delay = self.backoff[0]
        redirects = 0
        self._current = self._target
        while not self._stopping.is_set():
            started = time.monotonic()
            try:
                await self._session()
                redirects = 0
            except grpc.aio.AioRpcError as e:
                if self._stopping.is_set():
                    break
                to = _redirect(e, self._secure(self._current), bool(self._md), self._insecure_redirect_http)
                if to is not None:
                    redirects += 1
                    if redirects > MAX_REDIRECTS:
                        raise ClientError(
                            "failed_precondition",
                            f"worker redirected more than {MAX_REDIRECTS} times in a row: {e.details()}",
                        ) from None
                    log.info("pool %r is served by another shard; connecting to %s", self.pool, to)
                    self._current = to
                    continue
                redirects = 0
                if e.code() == grpc.StatusCode.UNAVAILABLE and self._current != self._target:
                    # The shard is unreachable: ask the configured endpoint where the pool is.
                    log.warning("shard at %s unreachable; returning to %s", self._current, self._target)
                    self._current = self._target
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
            if self._reconnect_now or time.monotonic() - started > 10 * self.backoff[1]:
                # Leadership moved (reconnect right away), or the last session was healthy.
                delay = self.backoff[0]
            self._reconnect_now = False
            try:
                await asyncio.wait_for(self._stopping.wait(), delay)
            except TimeoutError:
                pass
            delay = min(delay * 2, self.backoff[1])

    async def stop(self, grace: float | None = None) -> None:
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
            except TimeoutError:
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
            pb.WorkerMessage(
                register=pb.Register(
                    pool=self.pool,
                    slots=self.slots,
                    worker_name=self.name,
                    protocol_version=PROTOCOL_VERSION,
                    labels=self.labels,
                )
            )
        )

        async def requests() -> AsyncIterator[pb.WorkerMessage]:
            while True:
                msg = await outbox.get()
                if msg is _STOP:
                    return
                yield msg

        options = self._options
        if self._current != self._target and options:
            # A fixed server name belongs to the configured endpoint, not to the shard's.
            options = [o for o in options if o[0] not in _SERVER_NAME_OPTIONS]
        async with _channel(self._current, self._tls, self._credentials, options) as channel:
            stub = pb_grpc.WorkersStub(channel)
            call = stub.Work(requests(), metadata=self._md)
            self._call = call
            self.connections += 1
            try:
                async for msg in call:
                    kind = msg.WhichOneof("msg")
                    if kind == "registered":
                        reg = msg.registered
                        self.worker_id = reg.worker_id
                        self.lease_ttl = reg.lease_ttl_s if reg.HasField("lease_ttl_s") else None
                        # Version-1 servers send neither field.
                        self.granted_slots = reg.slots or self.slots
                        self.server_protocol = reg.protocol_version or 1
                        self.max_message_bytes = reg.max_message_bytes or None
                        if self.granted_slots < self.slots:
                            log.info("server granted %d of %d slots", self.granted_slots, self.slots)
                        self._registered.set()
                    elif kind == "lease":
                        self._start(msg.lease, outbox)
                    elif kind == "abort":
                        entry = self._running.pop(msg.abort.lease, None)
                        if entry is not None:
                            entry[1].cancelled.set()
                            entry[0].cancel()
                    elif kind == "shutdown":
                        self.last_shutdown = msg.shutdown.reason or "shutdown"
                        self._reconnect_now = msg.shutdown.not_leader
                        log.info("server is going away (%s); reconnecting", self.last_shutdown)
                        break
            except asyncio.CancelledError:
                if not self._stopping.is_set():
                    raise
            finally:
                self._call = None
                self._closed.set()
                outbox.put_nowait(_STOP)

    def _give_back(self, lease_id: int, reason: str, outbox: asyncio.Queue) -> None:
        if (self.server_protocol or 1) >= 2:
            outbox.put_nowait(pb.WorkerMessage(nack=pb.Nack(lease=lease_id, reason=reason)))
        else:
            outbox.put_nowait(pb.WorkerMessage(fail=pb.Fail(lease=lease_id, message=reason, permanent=False)))

    def _start(self, m: pb.Lease, outbox: asyncio.Queue) -> None:
        if self._draining or self._stopping.is_set():
            self._give_back(m.lease, "worker is stopping", outbox)
            return
        if len(self._running) >= (self.granted_slots or self.slots):
            self._give_back(m.lease, "worker has no free slot", outbox)
            return
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
            pool=m.pool or self.pool,
            priority=_priority_name(m.priority),
            lease_ttl=m.lease_ttl_s if m.HasField("lease_ttl_s") else self.lease_ttl,
        )
        task = asyncio.get_running_loop().create_task(self._run_lease(lease, outbox))
        self._running[lease.id] = (task, lease)

    async def _heartbeat(self, lease: Lease, outbox: asyncio.Queue) -> None:
        ttl = lease.lease_ttl or self.lease_ttl
        if not ttl:
            return
        period = max(ttl / 3, 0.01)
        while True:
            await asyncio.sleep(period)
            outbox.put_nowait(pb.WorkerMessage(heartbeat=pb.Heartbeat(lease=lease.id)))

    async def _run_lease(self, lease: Lease, outbox: asyncio.Queue) -> None:
        beat = asyncio.get_running_loop().create_task(self._heartbeat(lease, outbox))
        msg: pb.WorkerMessage | None = None
        try:
            out = await self.handler(lease)
            try:
                data, content_type = _encode(out, None)
            except (TypeError, ValueError) as e:
                raise Permanent(f"handler output is not JSON-serialisable: {e}") from None
            limit = self.max_message_bytes
            if limit is not None and len(data) + _MESSAGE_OVERHEAD > limit:
                raise Permanent(
                    f"output of {len(data)} bytes exceeds the server's message limit of {limit} bytes"
                )
            msg = pb.WorkerMessage(complete=pb.Complete(lease=lease.id, output=data, content_type=content_type))
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
        except Reject as e:
            self._running.pop(lease.id, None)
            self._give_back(lease.id, e.reason or "rejected by the handler", outbox)
        except CancelTask as e:
            if (self.server_protocol or 1) >= 2:
                msg = pb.WorkerMessage(cancelled=pb.Cancelled(lease=lease.id))
            else:
                msg = pb.WorkerMessage(
                    fail=pb.Fail(lease=lease.id, message=str(e) or "cancelled by the handler", permanent=True)
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
