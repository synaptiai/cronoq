"""Type stubs for the native module."""

from __future__ import annotations

import datetime
from typing import Any, Awaitable, Callable, Generator, Literal, Mapping, Optional, Sequence, Union

from typing_extensions import Self, TypeAlias, final

__version__: str
__all__ = [
    "AutoscaleConfig",
    "Dep",
    "EventStream",
    "Pool",
    "PoolSnapshot",
    "Priority",
    "Resource",
    "RetryPolicy",
    "Scheduler",
    "Snapshot",
    "StatusEvent",
    "Task",
    "TaskContext",
    "TaskHandle",
    "__version__",
]

_PriorityLike: TypeAlias = Union["Priority", int, str]
_TaskRefLike: TypeAlias = Union[str, int]
_TimeLike: TypeAlias = Union[datetime.datetime, float]
_Executor: TypeAlias = Callable[["TaskContext", Any], Any]
_MissPolicy: TypeAlias = Literal["run_late", "skip", "fail"]
_DepCondition: TypeAlias = Literal["success", "failure", "complete", "on_success", "on_failure", "on_complete"]

@final
class Priority:
    CRITICAL: Priority
    HIGH: Priority
    NORMAL: Priority
    LOW: Priority
    @property
    def name(self) -> str: ...
    @property
    def value(self) -> int: ...
    def __int__(self) -> int: ...
    def __lt__(self, value: Priority, /) -> bool: ...
    def __le__(self, value: Priority, /) -> bool: ...
    def __gt__(self, value: Priority, /) -> bool: ...
    def __ge__(self, value: Priority, /) -> bool: ...
    def __hash__(self) -> int: ...

@final
class RetryPolicy:
    max_attempts: int
    initial_backoff: float
    multiplier: float
    max_backoff: float
    jitter: bool
    def __new__(
        cls,
        max_attempts: int = 3,
        initial_backoff: float = 1.0,
        multiplier: float = 2.0,
        max_backoff: float = 60.0,
        jitter: bool = True,
    ) -> Self: ...
    @staticmethod
    def none() -> RetryPolicy: ...

@final
class AutoscaleConfig:
    interval: float
    target_utilization: float
    drain_time: float
    reservation_horizon: float
    stabilization_window: float
    cooldown: float
    max_step_up_abs: int
    max_step_down: int
    cost_per_worker_second: float
    slo: dict[str, float]
    def __new__(
        cls,
        interval: float = 1.0,
        target_utilization: float = 0.75,
        drain_time: float = 30.0,
        slo: Optional[Mapping[_PriorityLike, float]] = None,
        reservation_horizon: float = 60.0,
        stabilization_window: float = 60.0,
        cooldown: float = 30.0,
        max_step_up_abs: int = 4,
        max_step_down: int = 1,
        cost_per_worker_second: float = 0.0,
    ) -> Self: ...

@final
class Dep:
    def __new__(cls, target: _TaskRefLike, on: _DepCondition = "success") -> Self: ...

@final
class Resource:
    @staticmethod
    def semaphore(name: str, permits: int) -> Resource: ...
    @staticmethod
    def token_bucket(name: str, rate: float, burst: int) -> Resource: ...

@final
class Pool:
    name: str
    def __new__(
        cls,
        name: str,
        executor: _Executor,
        *,
        min: int = 1,
        max: int = 10,
        initial: Optional[int] = None,
        autoscale: Union[bool, AutoscaleConfig, None] = None,
        lease_ttl: Optional[float] = None,
        max_pending: Optional[int] = None,
        overflow: Literal["wait", "reject", "shed_lowest", "shed_oldest"] = "wait",
        retry: Optional[RetryPolicy] = None,
        unknown_dependency: Literal["reject", "await"] = "reject",
        await_timeout: Optional[float] = None,
        default_duration: float = 1.0,
        reservable_fraction: float = 0.5,
    ) -> Self: ...

@final
class Task:
    payload: Any
    key: Optional[str]
    def __new__(
        cls,
        payload: Any,
        *,
        priority: Optional[_PriorityLike] = None,
        pool: Optional[str] = None,
        kind: Optional[str] = None,
        key: Optional[str] = None,
        deadline_in: Optional[float] = None,
        deadline_at: Optional[_TimeLike] = None,
        start_in: Optional[float] = None,
        start_at: Optional[_TimeLike] = None,
        tolerance: Optional[float] = None,
        on_miss: Optional[_MissPolicy] = None,
        duration_hint: Optional[float] = None,
        timeout: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        after: Union[_TaskRefLike, Sequence[_TaskRefLike], None] = None,
        deps: Optional[Sequence[Dep]] = None,
        resources: Optional[Mapping[str, int]] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> Self: ...

@final
class TaskContext:
    id: int
    attempt: int
    kind: str
    key: Optional[str]
    pool: str
    tags: dict[str, str]
    timeout: Optional[float]
    def is_cancelled(self) -> bool: ...
    @property
    def deadline(self) -> Optional[float]: ...
    def heartbeat(self) -> None: ...

@final
class TaskHandle:
    id: int
    duplicate: bool
    def __await__(self) -> Generator[Any, None, Any]: ...
    def result(self) -> Awaitable[Any]: ...
    def done(self) -> bool: ...
    def cancel(self) -> None: ...

@final
class PoolSnapshot:
    name: str
    capacity: int
    busy: int
    held: int
    desired: int
    pending: int
    pending_by_class: list[int]
    ready: int
    reservations: int
    window: float
    dispatched: int
    succeeded: int
    failed: int
    retried: int
    priority_adherence: float
    fixed_start_hit_rate: float
    utilization: float
    schedule_efficiency: float
    wait_mean: float
    wait_p50: float
    wait_p90: float
    wait_p99: float
    run_p50: float
    run_p99: float
    worker_seconds: float
    cost: float

@final
class Snapshot:
    pools: list[PoolSnapshot]
    live_tasks: int
    running: int
    pending: int
    totals: dict[str, int]
    def pool(self, name: str) -> Optional[PoolSnapshot]: ...

@final
class StatusEvent:
    id: int
    pool: str
    key: Optional[str]
    status: str
    attempt: int
    at: float
    message: Optional[str]

@final
class EventStream:
    @property
    def lagged(self) -> int: ...
    @property
    def closed(self) -> bool: ...
    def __aiter__(self) -> Self: ...
    def __anext__(self) -> Awaitable[StatusEvent]: ...
    def close(self) -> None: ...
    def aclose(self) -> Awaitable[None]: ...
    def __aenter__(self) -> Awaitable[Self]: ...
    def __aexit__(self, *args: Any) -> Awaitable[None]: ...

@final
class Scheduler:
    @property
    def started(self) -> bool: ...
    @property
    def pools(self) -> list[str]: ...
    @property
    def default_pool(self) -> str: ...
    def __new__(
        cls,
        pools: Sequence[Pool],
        resources: Optional[Sequence[Resource]] = None,
        emit_transitions: bool = False,
        *,
        shutdown_grace: float = 5.0,
        event_capacity: int = 1024,
    ) -> Self: ...
    def start(self) -> Awaitable[None]: ...
    def submit(
        self,
        payload: Any,
        *,
        priority: Optional[_PriorityLike] = None,
        pool: Optional[str] = None,
        kind: Optional[str] = None,
        key: Optional[str] = None,
        deadline_in: Optional[float] = None,
        deadline_at: Optional[_TimeLike] = None,
        start_in: Optional[float] = None,
        start_at: Optional[_TimeLike] = None,
        tolerance: Optional[float] = None,
        on_miss: Optional[_MissPolicy] = None,
        duration_hint: Optional[float] = None,
        timeout: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        after: Union[_TaskRefLike, Sequence[_TaskRefLike], None] = None,
        deps: Optional[Sequence[Dep]] = None,
        resources: Optional[Mapping[str, int]] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> Awaitable[TaskHandle]: ...
    def enqueue(
        self,
        payload: Any,
        *,
        priority: Optional[_PriorityLike] = None,
        pool: Optional[str] = None,
        kind: Optional[str] = None,
        key: Optional[str] = None,
        deadline_in: Optional[float] = None,
        deadline_at: Optional[_TimeLike] = None,
        start_in: Optional[float] = None,
        start_at: Optional[_TimeLike] = None,
        tolerance: Optional[float] = None,
        on_miss: Optional[_MissPolicy] = None,
        duration_hint: Optional[float] = None,
        timeout: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        after: Union[_TaskRefLike, Sequence[_TaskRefLike], None] = None,
        deps: Optional[Sequence[Dep]] = None,
        resources: Optional[Mapping[str, int]] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> Awaitable[int]: ...
    def submit_batch(self, tasks: Sequence[Task]) -> Awaitable[list[TaskHandle]]: ...
    def cancel(self, target: _TaskRefLike) -> Awaitable[bool]: ...
    def status(self, target: _TaskRefLike) -> Awaitable[Optional[str]]: ...
    def handle(self, target: _TaskRefLike) -> Awaitable[Optional[TaskHandle]]: ...
    def snapshot(self) -> Awaitable[Snapshot]: ...
    def set_capacity(self, pool: str, capacity: int) -> Awaitable[None]: ...
    def add_recurring(
        self,
        name: str,
        cron: str,
        payload: Any,
        *,
        tz: Optional[str] = None,
        overlap: Literal["skip", "allow"] = "skip",
        tolerance: float = 1.0,
        on_miss: _MissPolicy = "run_late",
        priority: Optional[_PriorityLike] = None,
        pool: Optional[str] = None,
        kind: Optional[str] = None,
        duration_hint: Optional[float] = None,
        timeout: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        resources: Optional[Mapping[str, int]] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> Awaitable[None]: ...
    def remove_recurring(self, name: str) -> Awaitable[bool]: ...
    def events(self, on_lag: Literal["skip", "raise"] = "skip") -> EventStream: ...
    def shutdown(self, grace: float = 5.0) -> Awaitable[None]: ...
    def __aenter__(self) -> Awaitable[Self]: ...
    def __aexit__(self, *args: Any) -> Awaitable[None]: ...
