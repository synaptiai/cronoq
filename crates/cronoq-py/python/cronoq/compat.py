"""Drop-in replacement for the original Python ``ElasticPriorityScheduler`` prototype.

The API (``Task``, ``Priority``, ``submit``/``start``/``stop``/``get_metrics``/``get_cost``)
mirrors ``docs/prototype/elastic_scheduler.py``; scheduling is done by the native engine running
on a private event loop in a background thread. As in the prototype, a task "runs" by sleeping for
its ``duration``.

Differences from the prototype (all deliberate fixes):

* Fixed-start reservations count against pool capacity: up to half of ``max_workers`` (at least
  one) overlapping fixed tasks are accepted, instead of only one globally.
* Dependencies on ids that were not submitted yet wait for them (as before), but completed
  dependencies are never forgotten.
* Autoscaling is driven by queue pressure; ``scale_up_threshold`` and ``scale_down_threshold``
  are accepted for compatibility and ignored.
* Recorded start times are the actual dispatch times.
* ``schedule_quality`` is total work divided by the worker-seconds provisioned between the first
  start and the last completion, instead of using the worker count at the time of the call
  (which may have changed since).
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import threading
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Awaitable, Callable, List, Optional, TypeVar

from ._cronoq import AutoscaleConfig, Pool, Scheduler
from .errors import SubmitError

logger = logging.getLogger(__name__)

__all__ = [
    "ElasticPriorityScheduler",
    "Priority",
    "QualityMetrics",
    "ScalingConfig",
    "ScheduledTask",
    "Task",
]

EPSILON = 0.001
FIXED_SCHEDULE_BUFFER_SECONDS = 1.0

T = TypeVar("T")


class Priority(IntEnum):
    """Task priority levels (lower number = higher priority)."""

    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class Task:
    """A task to be scheduled (times are Unix timestamps)."""

    id: str
    duration: float
    priority: Priority = Priority.NORMAL
    arrival_time: float = 0.0
    fixed_start: Optional[float] = None
    deadline: Optional[float] = None
    dependencies: List[str] = field(default_factory=list)

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass
class ScheduledTask:
    """A task that has completed."""

    task: Task
    worker_id: int
    start_time: float
    end_time: float


@dataclass
class QualityMetrics:
    """Quality metrics, as defined by the prototype."""

    schedule_quality: float
    priority_adherence: float
    fixed_schedule_hits: float
    utilization: float
    avg_wait_time: float
    pending_count: int
    worker_count: int


@dataclass
class ScalingConfig:
    """Accepted for compatibility."""

    min_workers: int = 1
    max_workers: int = 10
    scale_up_threshold: float = 0.70
    scale_down_threshold: float = 0.90
    cooldown_seconds: float = 30.0
    worker_cost_per_second: float = 0.001


class ElasticPriorityScheduler:
    """Synchronous facade over :class:`cronoq.Scheduler` with the prototype's API.

    All public methods are thread-safe.
    """

    def __init__(
        self,
        initial_workers: int = 1,
        max_workers: int = 10,
        scale_up_threshold: float = 0.70,
        scale_down_threshold: float = 0.90,
        worker_cost_per_second: float = 0.001,
    ) -> None:
        self._initial = max(1, initial_workers)
        self._max = max(self._initial, max_workers)
        self._cost_rate = worker_cost_per_second
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self._lock = threading.Lock()
        self._completed: List[ScheduledTask] = []
        self._ws_first: Optional[float] = None  # worker-seconds at first start
        self._ws_last: Optional[float] = None  # worker-seconds at latest completion
        self._pending: dict[str, Task] = {}
        self._queued: List[Task] = []  # submitted before start()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._sched: Optional[Scheduler] = None
        self._start_time = 0.0
        self._stop_time: Optional[float] = None
        self._last: Optional[tuple[int, int, float, float]] = None  # pending, workers, adherence, cost

    # ------------------------------------------------------------------ lifecycle

    def _call(self, make: Callable[[], Awaitable[T]], timeout: float = 30.0) -> T:
        """Run `make()` on the scheduler's loop (native calls need a running loop)."""
        assert self._loop is not None

        async def run() -> T:
            return await make()

        return asyncio.run_coroutine_threadsafe(run(), self._loop).result(timeout)

    def start(self) -> None:
        """Start the scheduler (and submit tasks queued before start)."""
        if self._sched is not None:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="cronoq-compat", daemon=True)
        self._thread.start()
        pool = Pool(
            "default",
            self._execute,
            min=1,
            initial=self._initial,
            max=self._max,
            autoscale=AutoscaleConfig(
                drain_time=2.0,
                stabilization_window=5.0,
                cooldown=2.0,
                cost_per_worker_second=self._cost_rate,
            ),
            unknown_dependency="await",
        )
        sched = Scheduler([pool])
        self._call(sched.start)
        self._sched = sched
        self._start_time = time.time()
        self._stop_time = None
        with self._lock:
            queued, self._queued = self._queued, []
        for task in queued:
            self.submit(task)
        logger.info("Scheduler started")

    def stop(self) -> None:
        """Stop gracefully (running tasks get up to 2 seconds)."""
        if self._sched is None:
            return
        try:
            self._last = self._native_stats()
            sched = self._sched
            self._call(lambda: sched.shutdown(2.0))
        finally:
            assert self._loop is not None and self._thread is not None
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=2.0)
            self._sched = None
            self._stop_time = time.time()
        logger.info("Scheduler stopped")

    # ------------------------------------------------------------------ work

    async def _worker_seconds(self) -> Optional[float]:
        sched = self._sched
        if sched is None:
            return None
        return (await sched.snapshot()).pools[0].worker_seconds

    async def _execute(self, ctx: Any, task: Task) -> None:
        start = time.time()
        if self._ws_first is None:
            self._ws_first = await self._worker_seconds()
        await asyncio.sleep(task.duration)
        self._ws_last = await self._worker_seconds()
        with self._lock:
            self._pending.pop(task.id, None)
            self._completed.append(ScheduledTask(task=task, worker_id=0, start_time=start, end_time=time.time()))

    def submit(self, task: Task) -> bool:
        """Submit a task. Returns False if it was rejected (e.g. fixed-start conflict, cycle)."""
        if task.arrival_time == 0:
            task.arrival_time = time.time()
        if self._sched is None:
            with self._lock:
                self._queued.append(task)
                self._pending[task.id] = task
            return True
        try:
            sched = self._sched
            self._call(
                lambda: sched.enqueue(
                    task,
                    key=task.id,
                    priority=int(task.priority),
                    duration_hint=max(task.duration, EPSILON),
                    start_at=task.fixed_start,
                    tolerance=FIXED_SCHEDULE_BUFFER_SECONDS if task.fixed_start is not None else None,
                    deadline_at=task.deadline,
                    after=list(task.dependencies) or None,
                )
            )
        except SubmitError as e:
            logger.warning("Task %s rejected: %s", task.id, e)
            with self._lock:
                self._pending.pop(task.id, None)
            return False
        with self._lock:
            self._pending[task.id] = task
        return True

    # ------------------------------------------------------------------ metrics

    def _native_stats(self) -> tuple[int, int, float, float]:
        if self._sched is None:
            if self._last is not None:
                return self._last
            with self._lock:
                return (len(self._pending), self._initial, 1.0, 0.0)
        sched = self._sched
        snap = self._call(sched.snapshot)
        pool = snap.pools[0]
        return (snap.pending, pool.capacity, pool.priority_adherence, pool.cost)

    def get_metrics(self) -> QualityMetrics:
        """Prototype quality metrics computed over completed tasks."""
        pending, workers, adherence, _ = self._native_stats()
        with self._lock:
            completed = list(self._completed)
        if completed:
            total_work = sum(s.task.duration for s in completed)
            if self._ws_first is not None and self._ws_last is not None:
                provisioned = self._ws_last - self._ws_first
            else:
                makespan = max(s.end_time for s in completed) - min(s.start_time for s in completed)
                provisioned = makespan * max(workers, 1)
            quality = min(total_work / max(provisioned, EPSILON), 1.0)
        else:
            quality = 1.0 if pending == 0 else 0.5
        fixed = [s for s in completed if s.task.fixed_start is not None]
        hits = (
            sum(1 for s in fixed if abs(s.start_time - s.task.fixed_start) < FIXED_SCHEDULE_BUFFER_SECONDS) / len(fixed)
            if fixed
            else 1.0
        )
        end = self._stop_time or time.time()
        elapsed = end - self._start_time if self._start_time else 0.0
        if completed and elapsed > 0 and workers > 0:
            utilization = min(sum(s.task.duration for s in completed) / (workers * elapsed), 1.0)
        else:
            utilization = 0.0
        avg_wait = statistics.mean(s.start_time - s.task.arrival_time for s in completed) if completed else 0.0
        return QualityMetrics(
            schedule_quality=quality,
            priority_adherence=adherence,
            fixed_schedule_hits=hits,
            utilization=utilization,
            avg_wait_time=avg_wait,
            pending_count=pending,
            worker_count=workers,
        )

    def get_cost(self) -> float:
        """Total worker cost so far (worker-seconds x cost rate)."""
        return self._native_stats()[3]

    @property
    def completed_count(self) -> int:
        with self._lock:
            return len(self._completed)

    @property
    def completed_tasks(self) -> List[ScheduledTask]:
        with self._lock:
            return list(self._completed)

    @property
    def pending_tasks(self) -> List[Task]:
        with self._lock:
            return list(self._pending.values())
