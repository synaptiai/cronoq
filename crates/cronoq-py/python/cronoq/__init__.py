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
    Pool,
    PoolSnapshot,
    Priority,
    Resource,
    RetryPolicy,
    Scheduler,
    Snapshot,
    SqliteStore,
    StatusEvent,
    Task,
    TaskContext,
    TaskHandle,
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
)

__all__ = [
    "AutoscaleConfig",
    "Codec",
    "CronoqError",
    "Dep",
    "EventStream",
    "EventsLagged",
    "JsonCodec",
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
    "SqliteStore",
    "StatusEvent",
    "SubmitError",
    "Task",
    "TaskContext",
    "TaskFailed",
    "TaskHandle",
    "__version__",
]


def __getattr__(name: str):
    # `cronoq.client` needs the optional grpc dependencies; import it only on demand.
    if name == "client":
        import importlib

        return importlib.import_module(".client", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
