"""cronoq: an elastic, priority-aware task scheduler with a Rust core.

Example::

    import asyncio, cronoq

    async def run(ctx, payload):
        return payload * 2

    async def main():
        async with cronoq.Scheduler([cronoq.Pool("default", run, max=8)]) as sched:
            handle = await sched.submit(21, priority=cronoq.Priority.HIGH, deadline_in=5.0)
            print(await handle)  # 42

    asyncio.run(main())
"""

from ._cronoq import (
    AutoscaleConfig,
    Dep,
    Pool,
    PoolSnapshot,
    Priority,
    Resource,
    RetryPolicy,
    Scheduler,
    Snapshot,
    Task,
    TaskContext,
    TaskHandle,
    __version__,
)
from .errors import (
    CronoqError,
    Permanent,
    Retryable,
    SchedulerError,
    SubmitError,
    TaskFailed,
)

__all__ = [
    "AutoscaleConfig",
    "CronoqError",
    "Dep",
    "Permanent",
    "Pool",
    "PoolSnapshot",
    "Priority",
    "Resource",
    "Retryable",
    "RetryPolicy",
    "Scheduler",
    "SchedulerError",
    "Snapshot",
    "SubmitError",
    "Task",
    "TaskContext",
    "TaskFailed",
    "TaskHandle",
    "__version__",
]
