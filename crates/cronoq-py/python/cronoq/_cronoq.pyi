"""Type stubs for the native module."""

from __future__ import annotations

import datetime
from typing import Any, Awaitable, Callable, Generator, Literal, Mapping, Optional, Sequence, Union

__version__: str

PriorityLike = Union["Priority", int, str]
TaskRefLike = Union[str, int]
TimeLike = Union[datetime.datetime, float]
Executor = Callable[["TaskContext", Any], Any]

class Priority:
    CRITICAL: Priority
    HIGH: Priority
    NORMAL: Priority
    LOW: Priority
    def __int__(self) -> int: ...

class RetryPolicy:
    max_attempts: int
    initial_backoff: float
    multiplier: float
    max_backoff: float
    jitter: bool
    def __init__(
        self,
        max_attempts: int = 3,
        initial_backoff: float = 1.0,
        multiplier: float = 2.0,
        max_backoff: float = 60.0,
        jitter: bool = True,
    ) -> None: ...
    @staticmethod
    def none() -> RetryPolicy: ...

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
    def __init__(
        self,
        interval: float = 1.0,
        target_utilization: float = 0.75,
        drain_time: float = 30.0,
        slo: Optional[Mapping[PriorityLike, float]] = None,
        reservation_horizon: float = 60.0,
        stabilization_window: float = 60.0,
        cooldown: float = 30.0,
        max_step_up_abs: int = 4,
        max_step_down: int = 1,
        cost_per_worker_second: float = 0.0,
    ) -> None: ...

class Dep:
    def __init__(self, target: TaskRefLike, on: Literal["success", "failure", "complete"] = "success") -> None: ...

class Resource:
    @staticmethod
    def semaphore(name: str, permits: int) -> Resource: ...
    @staticmethod
    def token_bucket(name: str, rate: float, burst: int) -> Resource: ...

class Pool:
    name: str
    def __init__(
        self,
        name: str,
        executor: Executor,
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
    ) -> None: ...

class Task:
    payload: Any
    key: Optional[str]
    def __init__(
        self,
        payload: Any,
        *,
        priority: Optional[PriorityLike] = None,
        pool: Optional[str] = None,
        kind: Optional[str] = None,
        key: Optional[str] = None,
        deadline_in: Optional[float] = None,
        deadline_at: Optional[TimeLike] = None,
        start_in: Optional[float] = None,
        start_at: Optional[TimeLike] = None,
        tolerance: Optional[float] = None,
        on_miss: Optional[Literal["run_late", "skip", "fail"]] = None,
        duration_hint: Optional[float] = None,
        timeout: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        after: Union[TaskRefLike, Sequence[TaskRefLike], None] = None,
        deps: Optional[Sequence[Dep]] = None,
        resources: Optional[Mapping[str, int]] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> None: ...

class TaskContext:
    id: int
    attempt: int
    kind: str
    key: Optional[str]
    pool: str
    tags: dict[str, str]
    timeout: Optional[float]
    def is_cancelled(self) -> bool: ...
    def heartbeat(self) -> None: ...

class TaskHandle:
    id: int
    duplicate: bool
    def __await__(self) -> Generator[Any, None, Any]: ...
    def result(self) -> Awaitable[Any]: ...
    def done(self) -> bool: ...
    def cancel(self) -> None: ...

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

class Snapshot:
    pools: list[PoolSnapshot]
    live_tasks: int
    running: int
    pending: int
    totals: dict[str, int]
    def pool(self, name: str) -> Optional[PoolSnapshot]: ...

class Scheduler:
    started: bool
    pools: list[str]
    def __init__(
        self, pools: Sequence[Pool], resources: Optional[Sequence[Resource]] = None, emit_transitions: bool = False
    ) -> None: ...
    def start(self) -> Awaitable[None]: ...
    def submit(
        self,
        payload: Any,
        *,
        priority: Optional[PriorityLike] = None,
        pool: Optional[str] = None,
        kind: Optional[str] = None,
        key: Optional[str] = None,
        deadline_in: Optional[float] = None,
        deadline_at: Optional[TimeLike] = None,
        start_in: Optional[float] = None,
        start_at: Optional[TimeLike] = None,
        tolerance: Optional[float] = None,
        on_miss: Optional[Literal["run_late", "skip", "fail"]] = None,
        duration_hint: Optional[float] = None,
        timeout: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        after: Union[TaskRefLike, Sequence[TaskRefLike], None] = None,
        deps: Optional[Sequence[Dep]] = None,
        resources: Optional[Mapping[str, int]] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> Awaitable[TaskHandle]: ...
    def enqueue(self, payload: Any, **options: Any) -> Awaitable[int]: ...
    def submit_batch(self, tasks: Sequence[Task]) -> Awaitable[list[TaskHandle]]: ...
    def cancel(self, target: TaskRefLike) -> Awaitable[bool]: ...
    def status(self, target: TaskRefLike) -> Awaitable[Optional[str]]: ...
    def handle(self, target: TaskRefLike) -> Awaitable[Optional[TaskHandle]]: ...
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
        on_miss: Literal["run_late", "skip", "fail"] = "run_late",
        priority: Optional[PriorityLike] = None,
        pool: Optional[str] = None,
        kind: Optional[str] = None,
        duration_hint: Optional[float] = None,
        timeout: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        resources: Optional[Mapping[str, int]] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> Awaitable[None]: ...
    def remove_recurring(self, name: str) -> Awaitable[bool]: ...
    def shutdown(self, grace: float = 5.0) -> Awaitable[None]: ...
    async def __aenter__(self) -> Scheduler: ...
    async def __aexit__(self, *args: Any) -> None: ...
