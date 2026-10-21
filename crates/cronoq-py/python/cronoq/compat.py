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
* Submitting an id that is still pending is a no-op (returns ``True``); the prototype queued it
  twice. Submitting an id that already completed runs it again, as in the prototype.
* Tasks that did not finish before ``stop()`` stay pending and run after the next ``start()``.
"""

from __future__ import annotations

import asyncio
import logging
import math
import statistics
import threading
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, TypeVar

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
#: Fraction of ``max_workers`` that overlapping fixed-start tasks may occupy (engine default).
RESERVABLE_FRACTION = 0.5

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
        self._lock = threading.RLock()
        self._lifecycle = threading.RLock()
        self._completed: List[ScheduledTask] = []
        self._ws_first: Optional[float] = None  # worker-seconds at first start
        self._ws_last: Optional[float] = None  # worker-seconds at latest completion
        self._pending: Dict[str, Task] = {}
        self._queued: List[Task] = []  # waiting for start()
        self._generation: Dict[str, int] = {}  # submissions per id (for engine keys)
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
        """Start the scheduler (and submit tasks that are waiting for it)."""
        with self._lifecycle:
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
            # Worker-seconds restart with each engine.
            self._ws_first = self._ws_last = None
            self._sched = sched
            self._start_time = time.time()
            self._stop_time = None
            with self._lock:
                queued, self._queued = self._queued, []
            for task in queued:
                if not self._submit_native(task):
                    with self._lock:
                        self._pending.pop(task.id, None)
            logger.info("Scheduler started")

    def stop(self) -> None:
        """Stop gracefully: running tasks get up to 2 seconds. Tasks that did not finish stay
        pending and run after the next :meth:`start`."""
        with self._lifecycle:
            if self._sched is None:
                return
            try:
                self._last = self._native_stats()
                sched = self._sched
                self._call(lambda: sched.shutdown(2.0))
            finally:
                assert self._loop is not None and self._thread is not None
                self._loop.call_soon_threadsafe(self._loop.stop)
                self._thread.join(timeout=5.0)
                if not self._thread.is_alive():
                    self._loop.close()
                self._loop = None
                self._thread = None
                self._sched = None
                self._stop_time = time.time()
                with self._lock:
                    self._queued = list(self._pending.values())
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

    def _key(self, task_id: str) -> str:
        """Engine key of the latest submission of `task_id` (its first one: the id itself)."""
        gen = self._generation.get(task_id, 0)
        return task_id if gen <= 1 else f"{task_id}#{gen}"

    def _submit_native(self, task: Task) -> bool:
        sched = self._sched
        assert sched is not None
        with self._lock:
            self._generation[task.id] = self._generation.get(task.id, 0) + 1
            key = self._key(task.id)
            after = [self._key(d) for d in task.dependencies] or None
        try:
            self._call(
                lambda: sched.enqueue(
                    task,
                    key=key,
                    priority=int(task.priority),
                    duration_hint=max(task.duration, EPSILON),
                    start_at=task.fixed_start,
                    tolerance=FIXED_SCHEDULE_BUFFER_SECONDS if task.fixed_start is not None else None,
                    deadline_at=task.deadline,
                    after=after,
                )
            )
        except SubmitError as e:
            logger.warning("Task %s rejected: %s", task.id, e)
            return False
        return True

    def submit(self, task: Task) -> bool:
        """Submit a task. Returns False if it was rejected (e.g. fixed-start conflict, cycle)."""
        if task.arrival_time == 0:
            task.arrival_time = time.time()
        with self._lifecycle:
            with self._lock:
                if task.id in self._pending:
                    return True  # already queued or running
            if self._sched is None:
                with self._lock:
                    reason = self._prestart_rejection(task)
                    if reason is not None:
                        logger.warning("Task %s rejected: %s", task.id, reason)
                        return False
                    self._queued.append(task)
                    self._pending[task.id] = task
                return True
            with self._lock:
                self._pending[task.id] = task
            if not self._submit_native(task):
                with self._lock:
                    self._pending.pop(task.id, None)
                return False
            return True

    def _prestart_rejection(self, task: Task) -> Optional[str]:
        """The engine's admission checks for tasks waiting for :meth:`start`."""
        # Dependency cycles among waiting tasks (including depending on itself).
        deps: Dict[str, List[str]] = {t.id: t.dependencies for t in self._queued}
        deps[task.id] = task.dependencies
        stack, seen = list(task.dependencies), set()  # type: List[str], Set[str]
        while stack:
            d = stack.pop()
            if d == task.id:
                return "dependency cycle"
            if d not in seen:
                seen.add(d)
                stack.extend(deps.get(d, []))
        # Fixed-start reservations: at most `limit` may overlap.
        if task.fixed_start is not None:
            limit = max(1, math.floor(self._max * RESERVABLE_FRACTION))

            def interval(t: Task) -> tuple[float, float]:
                assert t.fixed_start is not None
                tol = FIXED_SCHEDULE_BUFFER_SECONDS
                return t.fixed_start - tol, t.fixed_start + max(t.duration, EPSILON) + tol

            s, e = interval(task)
            events = []
            for other in self._queued:
                if other.fixed_start is None:
                    continue
                os_, oe = interval(other)
                if os_ < e and oe > s:
                    events += [(max(os_, s), 1), (min(oe, e), -1)]
            cur = peak = 0
            for _, delta in sorted(events):
                cur += delta
                peak = max(peak, cur)
            if peak >= limit:
                return "fixed-start reservation conflict"
        return None

    # ------------------------------------------------------------------ metrics

    def _native_stats(self) -> tuple[int, int, float, float]:
        if self._sched is None:
            if self._last is not None:
                pending, workers, adherence, cost = self._last
                with self._lock:
                    return (len(self._pending), workers, adherence, cost)
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
            if self._ws_first is not None and self._ws_last is not None and self._ws_last > self._ws_first:
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
