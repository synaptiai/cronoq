"""Internal glue between the native engine and asyncio. Not part of the public API."""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import Any


async def _invoke(fn: Callable[[Any, Any], Awaitable[Any]], ctx: Any, payload: Any) -> Any:
    """Run one attempt, registering the asyncio task so the engine can cancel it."""
    ctx._set_task(asyncio.get_running_loop(), asyncio.current_task())
    return await fn(ctx, payload)


def wrap_sync(fn: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Awaitable[Any]]:
    """Adapt a plain callable: it runs in the loop's default thread pool, so blocking work
    does not stall the event loop. If it returns an awaitable, that is awaited too."""

    @functools.wraps(fn)
    async def run(ctx: Any, payload: Any) -> Any:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, fn, ctx, payload)
        if inspect.isawaitable(result):
            result = await result
        return result

    return run


async def _then_return(awaitable: Awaitable[Any], value: Any) -> Any:
    await awaitable
    return value
