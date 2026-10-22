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
    ``duplicate_key_in_batch``, ``invalid``.
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


class Permanent(CronoqError):
    """Raise from an executor to fail the task without retrying."""

    _cronoq_kind = "permanent"
