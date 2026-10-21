"""Exception types used by cronoq.

Executors control retries by raising :class:`Retryable` or :class:`Permanent`; any other
exception is treated as retryable. Awaiting a task that did not succeed raises
:class:`TaskFailed`.
"""

from __future__ import annotations

from typing import Optional


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


class SchedulerError(CronoqError):
    """The scheduler is shut down, misconfigured, or its store failed."""


class TaskFailed(CronoqError):
    """A task ended without a result.

    ``status`` is the terminal status name (``failed``, ``dead_lettered``, ``cancelled``,
    ``skipped``, ``missed``, ``shed``), or ``shutdown`` if the scheduler stopped first.
    """

    def __init__(self, status: str, message: str, task_id: Optional[int] = None) -> None:
        super().__init__(f"task {task_id} ended as {status}: {message}")
        self.status = status
        self.message = message
        self.task_id = task_id


class Retryable(Exception):
    """Raise from an executor to request a retry (subject to the retry policy).

    ``retry_after`` (seconds) delays the next attempt, e.g. for HTTP 429 responses.
    """

    _cronoq_kind = "retryable"

    def __init__(self, message: str = "", retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class Permanent(Exception):
    """Raise from an executor to fail the task without retrying."""

    _cronoq_kind = "permanent"
