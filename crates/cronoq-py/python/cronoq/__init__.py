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

from ._codec import Codec, JsonCodec
from ._cronoq import (
    AutoscaleConfig,
    Dep,
    EventStream,
    OrderingInfo,
    Pool,
    PoolSnapshot,
    Priority,
    PriorityConfig,
    Resource,
    RetryPolicy,
    Scheduler,
    Snapshot,
    SqliteStore,
    StatusEvent,
    Task,
    TaskContext,
    TaskHandle,
    Updated,
    UrgencyConfig,
    __version__,
)
from .errors import (
    CronoqError,
    EventsLagged,
    Permanent,
    Retryable,
    SchedulerError,
    SubmitError,
    TaskFailed,
    UpdateError,
)

__all__ = [
    "AutoscaleConfig",
    "Codec",
    "CronoqError",
    "Dep",
    "EventStream",
    "EventsLagged",
    "JsonCodec",
    "OrderingInfo",
    "Permanent",
    "Pool",
    "PoolSnapshot",
    "Priority",
    "PriorityConfig",
    "Resource",
    "Retryable",
    "RetryPolicy",
    "Scheduler",
    "SchedulerError",
    "Snapshot",
    "SqliteStore",
    "StatusEvent",
    "SubmitError",
    "Task",
    "TaskContext",
    "TaskFailed",
    "TaskHandle",
    "UpdateError",
    "Updated",
    "UrgencyConfig",
    "__version__",
]


def __getattr__(name: str):
    # `cronoq.client` needs the optional grpc dependencies; import it only on demand.
    if name == "client":
        import importlib

        return importlib.import_module(".client", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
