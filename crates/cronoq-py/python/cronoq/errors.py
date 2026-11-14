"""Exception types used by cronoq.

Executors control retries by raising :class:`Retryable` or :class:`Permanent`; any other
exception is treated as retryable. Awaiting a task that did not succeed raises
:class:`TaskFailed`. All exceptions here derive from :class:`CronoqError` and can be pickled
(e.g. sent across ``multiprocessing`` boundaries).
"""

from __future__ import annotations

from typing import Any


class CronoqError(Exception):
    """Base class for cronoq errors."""


class SubmitError(CronoqError):
    """A submission was rejected.

    ``code`` is one of: ``unknown_pool``, ``unknown_resource``, ``resource_exceeds_capacity``,
    ``reservation_conflict``, ``cycle``, ``unknown_dependency``, ``full``, ``overloaded``,
    ``duplicate_key_in_batch``, ``replace_mismatch``, ``key_conflict``, ``invalid``.

    For ``key_conflict`` (the key belongs to a task submitted with a different fingerprint, and
    the submission asked for that to be reported), ``task_id`` and ``task_status`` name that
    task. For a rejected batch, ``index`` is the position of the task that caused it.
    """

    def __init__(
        self,
        code: str,
        message: str,
        task_id: int | None = None,
        task_status: str | None = None,
        index: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.task_id = task_id
        self.task_status = task_status
        self.index = index

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.code, self.message, self.task_id, self.task_status, self.index))


class UpdateError(CronoqError):
    """An update of a task was refused.

    ``code`` is one of: ``not_found`` (no live task with this id or key: unknown, or already
    finished), ``fixed_start`` (tasks with a fixed start cannot be updated), ``invalid``.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.code, self.message))


class SchedulerError(CronoqError):
    """The scheduler is shut down or stopped, misconfigured, or its store failed."""


class TaskFailed(CronoqError):
    """A task ended without a result.

    ``status`` is the terminal status name (``failed``, ``dead_lettered``, ``cancelled``,
    ``skipped``, ``missed``, ``shed``), or ``shutdown`` if the scheduler stopped first.
    """

    def __init__(self, status: str, message: str, task_id: int | None = None) -> None:
        super().__init__(f"task {task_id} ended as {status}: {message}")
        self.status = status
        self.message = message
        self.task_id = task_id

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.status, self.message, self.task_id))


class EventsLagged(CronoqError):
    """Raised by an ``events(on_lag="raise")`` stream that fell behind: ``skipped`` events were
    dropped. Iterating again continues with the events that are still buffered."""

    def __init__(self, skipped: int) -> None:
        super().__init__(f"event stream fell behind; {skipped} events were dropped")
        self.skipped = skipped

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.skipped,))


class Retryable(CronoqError):
    """Raise from an executor to request a retry (subject to the retry policy).

    ``retry_after`` (seconds) delays the next attempt, e.g. for HTTP 429 responses.
    """

    _cronoq_kind = "retryable"

    def __init__(self, message: str = "", retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (str(self), self.retry_after))


class Release(CronoqError):
    """Raise from an executor or a worker handler to give the task back without a verdict.

    The executor cannot run the task now, and nothing is wrong with the task: a system it
    needs is down, the worker is shutting down. The task keeps its place in line and is
    offered again after ``delay`` seconds. The attempt does not count against the task's retry
    policy, up to the pool's ``max_releases`` (10 unless set) releases of one task, or the
    task's own ``max_releases``; beyond that a release counts as a retryable failure.
    """

    _cronoq_kind = "release"

    def __init__(self, delay: float = 0.0) -> None:
        super().__init__("released")
        self.delay = float(delay)

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.delay,))


class Permanent(CronoqError):
    """Raise from an executor to fail the task without retrying."""

    _cronoq_kind = "permanent"
