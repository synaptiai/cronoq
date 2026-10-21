#!/usr/bin/env python3
"""
Elastic Priority Scheduler

A production-ready task scheduler with:
- Variable task durations
- Priority-based scheduling with aging
- Fixed schedule support
- Quality-triggered elastic scaling
- Online task processing

MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import annotations

import heapq
import logging
import statistics
import threading
import time
from dataclasses import dataclass, field
from enum import IntEnum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

__all__ = [
    # Main interface
    "ElasticPriorityScheduler",
    # Data types
    "Task",
    "ScheduledTask",
    "Priority",
    "QualityMetrics",
    "ScalingConfig",
    # Components (for advanced usage)
    "HybridScheduler",
    "QualityMonitor",
    "PriorityCalculator",
    "Autoscaler",
]


# =============================================================================
# Constants
# =============================================================================

EPSILON = 0.001  # Minimum makespan to avoid division by zero
FIXED_SCHEDULE_BUFFER_SECONDS = 1.0  # Buffer before fixed task must start
SCHEDULER_POLL_INTERVAL = 0.1  # Seconds between scheduling checks
MONITOR_POLL_INTERVAL = 1.0  # Seconds between quality checks
MONITOR_REPORT_INTERVAL = 5.0  # Seconds between status reports
QUALITY_HISTORY_SIZE = 30  # Number of quality samples to retain
PRIORITY_REFRESH_INTERVAL = 1.0  # Seconds between priority recalculations
COMPLETED_RETENTION_LIMIT = 1000  # Max completed tasks to retain


# =============================================================================
# Design Notes: Concurrency Model
# =============================================================================
#
# Current Implementation (Threading):
#   - Uses threading.Thread for concurrent task execution
#   - RLock protects shared state (pending queue, completed list, workers)
#   - Daemon threads for scheduler loop, monitor loop, and task execution
#   - Graceful shutdown via threading.Event
#
# Trade-offs:
#   - Threading is simple and works well for I/O-bound simulated tasks
#   - Python's GIL means no true parallelism for CPU-bound work
#   - Each task spawns a new thread (acceptable for <1000 concurrent tasks)
#
# Alternative Approaches for Production:
#
#   1. asyncio (for I/O-bound workloads):
#      - Replace time.sleep() with asyncio.sleep()
#      - Use asyncio.Queue instead of heapq with locks
#      - Better for network I/O, database queries, API calls
#      - Single-threaded, no lock contention
#
#   2. ProcessPoolExecutor (for CPU-bound workloads):
#      - Replace threading.Thread with concurrent.futures.ProcessPoolExecutor
#      - True parallelism, bypasses GIL
#      - Higher overhead for task serialization
#
#   3. Thread Pool (for high task throughput):
#      - Use concurrent.futures.ThreadPoolExecutor
#      - Reuse threads instead of spawning new ones per task
#      - Reduces thread creation overhead
#
#   4. Condition Variables (for efficiency):
#      - Replace polling loops with threading.Condition
#      - Wake scheduler when tasks are added or completed
#      - Reduces CPU usage from polling
#
# =============================================================================


# =============================================================================
# Data Types
# =============================================================================


class Priority(IntEnum):
    """Task priority levels (lower number = higher priority)."""

    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class Task:
    """
    A task to be scheduled.

    Attributes:
        id: Unique identifier for the task.
        duration: Expected execution time in seconds.
        priority: Task priority level (CRITICAL > HIGH > NORMAL > LOW).
        arrival_time: Unix timestamp when task entered the system (auto-set if 0).
        fixed_start: If set, task must start at this exact time (Unix timestamp).
        deadline: If set, task should complete by this time for urgency boost.
        dependencies: List of task IDs that must complete before this task can start.
    """

    id: str
    duration: float
    priority: Priority = Priority.NORMAL
    arrival_time: float = 0.0
    fixed_start: float | None = None
    deadline: float | None = None
    dependencies: list[str] = field(default_factory=list)

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass
class ScheduledTask:
    """A task that has been scheduled/completed."""

    task: Task
    worker_id: int
    start_time: float
    end_time: float


@dataclass
class Worker:
    """A worker that executes tasks."""

    id: int
    available_at: float = 0.0
    current_task: Task | None = None
    task_start_time: float | None = None


@dataclass
class QualityMetrics:
    """
    Quality metrics for evaluating scheduler performance.

    Attributes:
        schedule_quality: Ratio of optimal makespan to actual makespan (0.0-1.0).
            1.0 means tasks completed in theoretical minimum time.
        priority_adherence: Fraction of tasks that started without violating
            priority order (0.0-1.0). Accounts for parallel workers - a LOW
            task on Worker B while HIGH runs on Worker A is not a violation.
        fixed_schedule_hits: Fraction of fixed-schedule tasks that started
            within the buffer window of their target time (0.0-1.0).
        utilization: Fraction of total worker-time spent executing tasks (0.0-1.0).
        avg_wait_time: Mean time tasks spent in queue before starting (seconds).
        pending_count: Number of tasks currently waiting to be scheduled.
        worker_count: Current number of active workers.
    """

    schedule_quality: float
    priority_adherence: float
    fixed_schedule_hits: float
    utilization: float
    avg_wait_time: float
    pending_count: int
    worker_count: int


@dataclass
class ScalingConfig:
    """Configuration for elastic scaling."""

    min_workers: int = 1
    max_workers: int = 10
    scale_up_threshold: float = 0.70
    scale_down_threshold: float = 0.90
    cooldown_seconds: float = 30.0
    worker_cost_per_second: float = 0.001


# =============================================================================
# Priority Calculator
# =============================================================================


class PriorityCalculator:
    """
    Calculates effective priority with aging and deadline urgency.

    Aging prevents starvation: tasks waiting longer get priority boost.
    """

    BASE_WEIGHTS: dict[Priority, int] = {
        Priority.CRITICAL: 10000,
        Priority.HIGH: 1000,
        Priority.NORMAL: 100,
        Priority.LOW: 10,
    }
    AGING_RATE = 2.0  # Priority boost per second of waiting
    MAX_AGE_BONUS = 800  # Cap to prevent LOW exceeding CRITICAL
    OVERDUE_URGENCY = 5000
    DEADLINE_URGENCY_FACTOR = 2000

    def calculate(self, task: Task, current_time: float) -> float:
        """Returns effective priority (higher = more urgent)."""
        if task.fixed_start is not None:
            buffer = max(task.duration * 0.1, FIXED_SCHEDULE_BUFFER_SECONDS)
            if current_time >= task.fixed_start - buffer:
                return float("inf")

        base = self.BASE_WEIGHTS[task.priority]

        wait_time = max(0, current_time - task.arrival_time)
        age_bonus = min(wait_time * self.AGING_RATE, self.MAX_AGE_BONUS)

        urgency = 0.0
        if task.deadline is not None:
            slack = task.deadline - current_time - task.duration
            if slack <= 0:
                urgency = self.OVERDUE_URGENCY
            elif slack < task.duration:
                urgency = self.DEADLINE_URGENCY_FACTOR / max(slack, EPSILON)

        return base + age_bonus + urgency


# =============================================================================
# Fixed Schedule Manager
# =============================================================================


@dataclass
class TimeSlot:
    """A reserved time slot for a fixed-schedule task."""

    start: float
    end: float
    task_id: str


class FixedScheduleManager:
    """Manages fixed schedule reservations."""

    def __init__(self) -> None:
        self._reservations: list[TimeSlot] = []

    def reserve(self, task: Task) -> bool:
        """Reserve time slot for fixed-schedule task. Returns False if conflict."""
        if task.fixed_start is None:
            return True

        slot = TimeSlot(
            start=task.fixed_start,
            end=task.fixed_start + task.duration,
            task_id=task.id,
        )

        if self._has_conflict(slot):
            return False

        self._reservations.append(slot)
        self._reservations.sort(key=lambda x: x.start)
        return True

    def _has_conflict(self, new_slot: TimeSlot) -> bool:
        for existing in self._reservations:
            overlaps = new_slot.start < existing.end and new_slot.end > existing.start
            different_task = new_slot.task_id != existing.task_id
            if overlaps and different_task:
                return True
        return False

    def remove_reservation(self, task_id: str) -> None:
        self._reservations = [s for s in self._reservations if s.task_id != task_id]


# =============================================================================
# Quality Monitor
# =============================================================================


class QualityMonitor:
    """Monitors schedule quality and provides scaling signals."""

    def __init__(
        self,
        scale_up_threshold: float = 0.70,
        scale_down_threshold: float = 0.90,
    ) -> None:
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self._quality_history: list[float] = []

    def calculate(
        self,
        completed: list[ScheduledTask],
        pending_count: int,
        workers: list[Worker],
        current_time: float,
        start_time: float,
    ) -> QualityMetrics:
        """Calculate comprehensive quality metrics."""
        num_workers = len(workers)

        if completed:
            total_work = sum(scheduled.task.duration for scheduled in completed)
            first_start = min(scheduled.start_time for scheduled in completed)
            last_end = max(scheduled.end_time for scheduled in completed)
            actual_makespan = last_end - first_start

            lower_bound = total_work / num_workers if num_workers > 0 else total_work
            schedule_quality = min(lower_bound / max(actual_makespan, EPSILON), 1.0)
        else:
            schedule_quality = 1.0 if pending_count == 0 else 0.5

        priority_adherence = self._calc_priority_adherence(completed)
        fixed_schedule_hits = self._calc_fixed_schedule_hits(completed)

        elapsed = current_time - start_time
        if elapsed > 0 and num_workers > 0 and completed:
            total_work_time = sum(scheduled.task.duration for scheduled in completed)
            utilization = min(total_work_time / (num_workers * elapsed), 1.0)
        else:
            utilization = 0.0

        if completed:
            wait_times = [
                scheduled.start_time - scheduled.task.arrival_time
                for scheduled in completed
            ]
            avg_wait = statistics.mean(wait_times)
        else:
            avg_wait = 0.0

        return QualityMetrics(
            schedule_quality=schedule_quality,
            priority_adherence=priority_adherence,
            fixed_schedule_hits=fixed_schedule_hits,
            utilization=utilization,
            avg_wait_time=avg_wait,
            pending_count=pending_count,
            worker_count=num_workers,
        )

    def _calc_priority_adherence(self, completed: list[ScheduledTask]) -> float:
        """
        Calculate priority adherence accounting for parallel workers.

        A violation occurs when a lower-priority task starts while a higher-priority
        task was available (had arrived) and no worker was executing it yet.
        With parallel workers, it's valid for a LOW task to start on Worker B
        while a HIGH task runs on Worker A.
        """
        if len(completed) < 2:
            return 1.0

        violations = 0
        comparisons = 0

        for scheduled in completed:
            task = scheduled.task
            start_time = scheduled.start_time

            # Find higher-priority tasks that were available when this task started
            for other in completed:
                if other.task.id == task.id:
                    continue

                other_task = other.task
                # Only compare if other task has higher priority (lower number)
                if other_task.priority >= task.priority:
                    continue

                # Was the higher-priority task available when this one started?
                # (i.e., had it arrived before this task started?)
                if other_task.arrival_time > start_time:
                    continue  # Higher-priority task hadn't arrived yet - no violation

                # Was the higher-priority task already running or completed by then?
                # If it started before or at the same time, no violation
                if other.start_time <= start_time:
                    continue  # Higher-priority task was already being processed

                # Violation: higher-priority task was available but we started
                # a lower-priority task instead (and higher-priority wasn't running)
                violations += 1
                comparisons += 1
                break  # One violation per task is enough

            # Count tasks that had no violations as correct
            if comparisons == 0 or (comparisons > 0 and violations < comparisons):
                pass  # Will be handled by the ratio

        # Calculate based on tasks that started correctly vs. violations
        total_tasks = len(completed)
        if total_tasks == 0:
            return 1.0

        return max(0.0, 1.0 - (violations / total_tasks))

    def _calc_fixed_schedule_hits(self, completed: list[ScheduledTask]) -> float:
        fixed_tasks = [s for s in completed if s.task.fixed_start is not None]
        if not fixed_tasks:
            return 1.0

        on_time = sum(
            1
            for s in fixed_tasks
            if s.task.fixed_start is not None
            and abs(s.start_time - s.task.fixed_start) < FIXED_SCHEDULE_BUFFER_SECONDS
        )
        return on_time / len(fixed_tasks)

    def record_quality(self, quality: float) -> None:
        """Record a quality sample for trend analysis."""
        self._quality_history.append(quality)
        if len(self._quality_history) > QUALITY_HISTORY_SIZE:
            self._quality_history = self._quality_history[-QUALITY_HISTORY_SIZE:]

    def should_scale_up(self, quality: float) -> bool:
        """Check if quality trend warrants adding workers."""
        if len(self._quality_history) >= 3:
            return statistics.mean(self._quality_history[-3:]) < self.scale_up_threshold
        return quality < self.scale_up_threshold

    def should_scale_down(self, utilization: float) -> bool:
        """Check if quality trend allows removing workers."""
        if len(self._quality_history) >= 5:
            recent = statistics.mean(self._quality_history[-5:])
            return recent > self.scale_down_threshold and utilization < 0.5
        return False


# =============================================================================
# Hybrid Scheduler
# =============================================================================


@dataclass
class PendingTask:
    """A task in the pending queue with its scheduling metadata."""

    priority: float  # Negative for max-heap behavior
    sequence: int  # Tie-breaker for equal priorities
    task: Task

    def __lt__(self, other: PendingTask) -> bool:
        return (self.priority, self.sequence) < (other.priority, other.sequence)


class HybridScheduler:
    """Main scheduling engine combining greedy dispatch with quality monitoring."""

    def __init__(
        self,
        initial_workers: int = 1,
        max_workers: int = 10,
    ) -> None:
        self.max_workers = max_workers
        self.workers = [Worker(id=i) for i in range(initial_workers)]

        self._pending: list[PendingTask] = []
        self._completed: list[ScheduledTask] = []
        self._task_counter = 0
        self._last_priority_refresh = 0.0

        self._fixed_schedule_manager = FixedScheduleManager()
        self._priority_calculator = PriorityCalculator()

        self.current_time = time.time()
        self._lock = threading.RLock()

    def add_task(self, task: Task) -> bool:
        """Add a task to the scheduler."""
        with self._lock:
            if not self._fixed_schedule_manager.reserve(task):
                logger.warning(f"Task {task.id} rejected: fixed schedule conflict")
                return False

            if self._would_create_cycle(task):
                logger.warning(f"Task {task.id} rejected: dependency cycle detected")
                return False

            priority = self._priority_calculator.calculate(task, self.current_time)
            pending = PendingTask(
                priority=-priority,
                sequence=self._task_counter,
                task=task,
            )
            heapq.heappush(self._pending, pending)
            self._task_counter += 1
            logger.debug(f"Task {task.id} added (priority={priority:.1f})")
            return True

    def _would_create_cycle(self, new_task: Task) -> bool:
        """Check if adding new_task would create a dependency cycle using DFS."""
        if not new_task.dependencies:
            return False

        task_map: dict[str, Task] = {new_task.id: new_task}
        for pending in self._pending:
            task_map[pending.task.id] = pending.task

        visited: set[str] = set()
        visiting: set[str] = set()

        def has_cycle(task_id: str) -> bool:
            if task_id in visiting:
                return True
            if task_id in visited:
                return False

            task = task_map.get(task_id)
            if task is None:
                return False

            visiting.add(task_id)
            for dep_id in task.dependencies:
                if has_cycle(dep_id):
                    return True
            visiting.remove(task_id)
            visited.add(task_id)
            return False

        return has_cycle(new_task.id)

    def schedule_next(self) -> ScheduledTask | None:
        """Schedule the next task on an available worker."""
        with self._lock:
            worker = self._get_available_worker()
            if worker is None or worker.current_task is not None:
                return None

            self._refresh_priorities()

            task = self._get_next_ready_task()
            if task is None:
                return None

            start_time = self.current_time
            if task.fixed_start is not None:
                start_time = max(start_time, task.fixed_start)

            scheduled = ScheduledTask(
                task=task,
                worker_id=worker.id,
                start_time=start_time,
                end_time=start_time + task.duration,
            )

            worker.available_at = scheduled.end_time
            worker.current_task = task
            worker.task_start_time = start_time

            logger.info(
                f"Scheduled {task.id} on worker {worker.id} "
                f"(priority={task.priority.name}, duration={task.duration:.1f}s)"
            )
            return scheduled

    def complete_task(self, task_id: str) -> None:
        """Mark a task as completed."""
        with self._lock:
            for worker in self.workers:
                if worker.current_task and worker.current_task.id == task_id:
                    start_time = (
                        worker.task_start_time
                        if worker.task_start_time is not None
                        else self.current_time - worker.current_task.duration
                    )
                    scheduled = ScheduledTask(
                        task=worker.current_task,
                        worker_id=worker.id,
                        start_time=start_time,
                        end_time=self.current_time,
                    )
                    self._completed.append(scheduled)
                    if len(self._completed) > COMPLETED_RETENTION_LIMIT:
                        self._completed = self._completed[-COMPLETED_RETENTION_LIMIT:]
                    self._fixed_schedule_manager.remove_reservation(task_id)
                    worker.current_task = None
                    worker.task_start_time = None
                    worker.available_at = self.current_time
                    logger.info(f"Completed {task_id}")
                    return

    def _get_available_worker(self) -> Worker | None:
        for worker in self.workers:
            if worker.current_task is None and worker.available_at <= self.current_time:
                return worker
        return None

    def _get_next_ready_task(self) -> Task | None:
        deferred_tasks: list[PendingTask] = []
        result: Task | None = None

        completed_ids = {scheduled.task.id for scheduled in self._completed}

        while self._pending:
            pending = heapq.heappop(self._pending)
            task = pending.task

            deps_satisfied = all(dep in completed_ids for dep in task.dependencies)
            fixed_ok = (
                task.fixed_start is None
                or task.fixed_start <= self.current_time + FIXED_SCHEDULE_BUFFER_SECONDS
            )

            if deps_satisfied and fixed_ok:
                result = task
                break

            deferred_tasks.append(pending)

        for deferred in deferred_tasks:
            heapq.heappush(self._pending, deferred)

        return result

    def _refresh_priorities(self) -> None:
        if self.current_time - self._last_priority_refresh < PRIORITY_REFRESH_INTERVAL:
            return

        refreshed: list[PendingTask] = []
        while self._pending:
            pending = heapq.heappop(self._pending)
            new_priority = self._priority_calculator.calculate(
                pending.task, self.current_time
            )
            refreshed.append(
                PendingTask(
                    priority=-new_priority,
                    sequence=pending.sequence,
                    task=pending.task,
                )
            )

        for pending in refreshed:
            heapq.heappush(self._pending, pending)

        self._last_priority_refresh = self.current_time

    def add_worker(self) -> bool:
        """Add a worker if under max limit."""
        with self._lock:
            if len(self.workers) >= self.max_workers:
                return False
            new_id = max(w.id for w in self.workers) + 1 if self.workers else 0
            self.workers.append(Worker(id=new_id, available_at=self.current_time))
            logger.info(f"Added worker {new_id} (total: {len(self.workers)})")
            return True

    def remove_worker(self) -> bool:
        """Remove an idle worker if more than 1 remain. Prefers removing newest workers."""
        with self._lock:
            if len(self.workers) <= 1:
                return False
            for i in range(len(self.workers) - 1, -1, -1):
                worker = self.workers[i]
                if worker.current_task is None:
                    removed = self.workers.pop(i)
                    logger.info(
                        f"Removed worker {removed.id} (total: {len(self.workers)})"
                    )
                    return True
            return False

    @property
    def pending_tasks(self) -> list[Task]:
        """Get list of pending tasks."""
        return [pending.task for pending in self._pending]

    @property
    def completed_tasks(self) -> list[ScheduledTask]:
        """Get copy of completed tasks."""
        return self._completed.copy()

    @property
    def num_workers(self) -> int:
        """Get current worker count."""
        return len(self.workers)


# =============================================================================
# Autoscaler
# =============================================================================


class Autoscaler:
    """Quality-triggered elastic scaling with hysteresis."""

    def __init__(
        self,
        config: ScalingConfig,
        scheduler: HybridScheduler,
        quality_monitor: QualityMonitor,
    ) -> None:
        self.config = config
        self.scheduler = scheduler
        self.monitor = quality_monitor
        self._last_scale_time = 0.0
        self._total_cost = 0.0

    def evaluate(self, metrics: QualityMetrics, current_time: float) -> str:
        """Evaluate and perform scaling if needed."""
        if current_time - self._last_scale_time < self.config.cooldown_seconds:
            return "cooldown"

        quality = metrics.schedule_quality
        current_workers = self.scheduler.num_workers

        self.monitor.record_quality(quality)

        if self.monitor.should_scale_up(quality):
            if current_workers < self.config.max_workers:
                if self.scheduler.add_worker():
                    self._last_scale_time = current_time
                    return "scale_up"

        elif self.monitor.should_scale_down(metrics.utilization):
            if current_workers > self.config.min_workers:
                if self.scheduler.remove_worker():
                    self._last_scale_time = current_time
                    return "scale_down"

        return "no_change"

    def update_cost(self, elapsed: float) -> None:
        """Update running cost based on elapsed time."""
        workers = self.scheduler.num_workers
        self._total_cost += workers * elapsed * self.config.worker_cost_per_second

    @property
    def total_cost(self) -> float:
        """Get total cost incurred."""
        return self._total_cost


# =============================================================================
# Elastic Priority Scheduler (Main Interface)
# =============================================================================


class ElasticPriorityScheduler:
    """
    Production-ready task scheduler with elastic scaling.

    Features:
        - Priority-based scheduling with aging to prevent starvation
        - Fixed-schedule support for tasks that must start at specific times
        - Quality-triggered autoscaling (adds/removes workers based on metrics)
        - Dependency management with cycle detection
        - Graceful shutdown with in-flight task handling

    Args:
        initial_workers: Number of workers to start with.
        max_workers: Maximum workers allowed during scale-up.
        scale_up_threshold: Add workers when quality drops below this (0.0-1.0).
        scale_down_threshold: Remove workers when quality exceeds this (0.0-1.0).

    Example:
        >>> scheduler = ElasticPriorityScheduler(initial_workers=1, max_workers=4)
        >>> scheduler.start()
        >>> scheduler.submit(Task(id="task1", duration=10.0, priority=Priority.HIGH))
        >>> time.sleep(30)
        >>> print(scheduler.get_metrics())
        >>> scheduler.stop()

    Thread Safety:
        All public methods are thread-safe. Tasks can be submitted from any thread.
    """

    def __init__(
        self,
        initial_workers: int = 1,
        max_workers: int = 10,
        scale_up_threshold: float = 0.70,
        scale_down_threshold: float = 0.90,
    ) -> None:
        self._scheduler = HybridScheduler(
            initial_workers=initial_workers,
            max_workers=max_workers,
        )

        self._quality_monitor = QualityMonitor(
            scale_up_threshold=scale_up_threshold,
            scale_down_threshold=scale_down_threshold,
        )

        config = ScalingConfig(
            min_workers=1,
            max_workers=max_workers,
            scale_up_threshold=scale_up_threshold,
            scale_down_threshold=scale_down_threshold,
        )
        self._autoscaler = Autoscaler(config, self._scheduler, self._quality_monitor)

        self._running = False
        self._start_time = 0.0
        self._threads: list[threading.Thread] = []
        self._shutdown_event = threading.Event()

    def submit(self, task: Task) -> bool:
        """Submit a task for scheduling."""
        if task.arrival_time == 0:
            task.arrival_time = time.time()
        return self._scheduler.add_task(task)

    def start(self) -> None:
        """Start the scheduler."""
        self._threads = []  # Clear old thread references
        self._shutdown_event.clear()  # Reset shutdown signal
        self._running = True
        self._start_time = time.time()
        self._scheduler.current_time = self._start_time

        scheduler_thread = threading.Thread(
            target=self._scheduler_loop, daemon=True, name="scheduler"
        )
        scheduler_thread.start()
        self._threads.append(scheduler_thread)

        monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name="monitor"
        )
        monitor_thread.start()
        self._threads.append(monitor_thread)

        logger.info("Scheduler started")

    def stop(self) -> None:
        """Stop the scheduler gracefully."""
        self._running = False
        self._shutdown_event.set()  # Signal running tasks to stop
        for thread in self._threads:
            thread.join(timeout=2.0)
        logger.info("Scheduler stopped")

    def _scheduler_loop(self) -> None:
        while self._running:
            self._scheduler.current_time = time.time()

            scheduled = self._scheduler.schedule_next()
            if scheduled:
                self._dispatch_task(scheduled)

            time.sleep(SCHEDULER_POLL_INTERVAL)

    def _dispatch_task(self, scheduled: ScheduledTask) -> None:
        """Dispatch a scheduled task for execution."""

        def execute() -> None:
            # Use event.wait() for interruptible sleep
            if not self._shutdown_event.wait(timeout=scheduled.task.duration):
                self._scheduler.complete_task(scheduled.task.id)

        thread = threading.Thread(target=execute, daemon=True)
        thread.start()

    def _monitor_loop(self) -> None:
        last_report = time.time()

        while self._running:
            current = time.time()
            self._scheduler.current_time = current

            metrics = self._quality_monitor.calculate(
                self._scheduler.completed_tasks,
                len(self._scheduler.pending_tasks),
                self._scheduler.workers,
                current,
                self._start_time,
            )

            self._autoscaler.evaluate(metrics, current)
            self._autoscaler.update_cost(MONITOR_POLL_INTERVAL)

            if current - last_report >= MONITOR_REPORT_INTERVAL:
                logger.info(
                    f"Quality={metrics.schedule_quality:.2%} | "
                    f"Workers={metrics.worker_count} | "
                    f"Pending={metrics.pending_count} | "
                    f"Completed={len(self._scheduler.completed_tasks)}"
                )
                last_report = current

            time.sleep(MONITOR_POLL_INTERVAL)

    def get_metrics(self) -> QualityMetrics:
        """Get current quality metrics."""
        return self._quality_monitor.calculate(
            self._scheduler.completed_tasks,
            len(self._scheduler.pending_tasks),
            self._scheduler.workers,
            time.time(),
            self._start_time,
        )

    def get_cost(self) -> float:
        """Get total cost incurred."""
        return self._autoscaler.total_cost

    @property
    def completed_count(self) -> int:
        """Get number of completed tasks."""
        return len(self._scheduler.completed_tasks)
