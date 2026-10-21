"""Regression tests for bugs found in review (each failed before its fix)."""

from __future__ import annotations

import asyncio
import gc
import pickle
import subprocess
import sys
import textwrap
import time
import weakref
from typing import Any, List

import pytest

import cronoq
from cronoq.compat import ElasticPriorityScheduler
from cronoq.compat import Task as CompatTask


async def echo(ctx: Any, p: Any) -> Any:
    return p


# ---------------------------------------------------------------------------------------------
# Native bindings


class Big:
    def __init__(self) -> None:
        self.data = bytearray(100_000)


async def test_failed_attempts_do_not_leak_frames() -> None:
    refs: List[weakref.ref] = []

    async def bad(ctx: Any, p: Any) -> Any:
        big = Big()
        refs.append(weakref.ref(big))
        raise cronoq.Permanent("nope")

    async with cronoq.Scheduler([cronoq.Pool("default", bad, max=4)]) as s:
        handles = [await s.submit(i) for i in range(20)]
        for h in handles:
            with pytest.raises(cronoq.TaskFailed):
                await h
    del handles
    gc.collect()
    gc.collect()
    assert sum(1 for r in refs if r() is not None) == 0


async def test_api_usable_while_shutting_down() -> None:
    s: Any = None
    seen: List[Any] = []

    async def run(ctx: Any, p: Any) -> Any:
        await asyncio.sleep(0.3)
        snap = await s.snapshot()
        seen.append(snap.live_tasks)
        seen.append(await s.status(ctx.id))
        return "ok"

    s = cronoq.Scheduler([cronoq.Pool("default", run, max=2)])
    await s.start()
    h = await s.submit(1)
    await asyncio.sleep(0.05)
    await s.shutdown(3.0)
    assert await h == "ok"
    assert seen == [1, "running"]
    assert not s.started


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(timeout=1e20),
        dict(deadline_in=1e20),
        dict(start_in=float("inf")),
        dict(duration_hint=-1.0),
        dict(tolerance=float("nan")),
    ],
)
async def test_bad_durations_raise_value_error(kwargs: Any) -> None:
    async with cronoq.Scheduler([cronoq.Pool("default", echo)]) as s:
        with pytest.raises(ValueError):
            await s.submit(1, **kwargs)
        assert await (await s.submit(2)) == 2


def test_bad_configs_raise_value_error() -> None:
    bad = [
        lambda: cronoq.RetryPolicy(initial_backoff=1e30),
        lambda: cronoq.RetryPolicy(multiplier=-1.0),
        lambda: cronoq.RetryPolicy(multiplier=float("nan")),
        lambda: cronoq.AutoscaleConfig(slo={"high": float("nan")}),
        lambda: cronoq.AutoscaleConfig(slo={"high": -1.0}),
        lambda: cronoq.AutoscaleConfig(target_utilization=0.0),
        lambda: cronoq.AutoscaleConfig(interval=0.0),
        lambda: cronoq.Pool("default", echo, reservable_fraction=5.0),
        lambda: cronoq.Pool("default", echo, lease_ttl=0.0),
        lambda: cronoq.Pool("default", echo, min=0, max=0),
        lambda: cronoq.Resource.token_bucket("x", -1.0, 1),
        lambda: cronoq.Resource.token_bucket("x", float("nan"), 1),
        lambda: cronoq.Scheduler([cronoq.Pool("a", echo), cronoq.Pool("a", echo)]),
        lambda: cronoq.Task(1, priority=True),
    ]
    for make in bad:
        with pytest.raises((ValueError, TypeError)):
            make()


async def test_shutdown_with_infinite_grace_and_recurring_tolerance() -> None:
    s = cronoq.Scheduler([cronoq.Pool("default", echo)])
    await s.start()
    with pytest.raises(ValueError):
        await s.add_recurring("r", "* * * * *", 1, tolerance=1e20)
    assert await s.shutdown(float("inf")) is None
    assert not s.started


def test_priority_is_hashable_and_ordered() -> None:
    cfg = cronoq.AutoscaleConfig(slo={cronoq.Priority.HIGH: 1.0, "low": 30.0})
    assert cfg.slo == {"high": 1.0, "low": 30.0}
    assert len({cronoq.Priority.HIGH, cronoq.Priority.HIGH, cronoq.Priority.LOW}) == 2
    assert cronoq.Priority.CRITICAL < cronoq.Priority.LOW
    assert cronoq.Priority.HIGH.name == "HIGH" and cronoq.Priority.HIGH.value == 1


def test_scheduler_stops_when_its_loop_is_closed() -> None:
    script = textwrap.dedent(
        """
        import asyncio, cronoq, time
        calls = 0
        async def run(ctx, p):
            global calls
            calls += 1
            return p
        s = cronoq.Scheduler([cronoq.Pool("default", run, max=2)])
        async def first():
            await s.start()
            assert await (await s.submit(1)) == 1
            await s.submit(2, start_in=0.3, key="late")
        asyncio.run(first())   # exits without shutdown
        time.sleep(0.8)
        async def second():
            try:
                await s.submit(3)
            except cronoq.SchedulerError as e:
                print("rejected:", e)
        asyncio.run(second())
        print("calls", calls, "started", s.started)
        """
    )
    out = subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert out.returncode == 0, out.stderr
    assert "rejected:" in out.stdout and "event loop" in out.stdout, out.stdout
    assert "calls 1 started False" in out.stdout, out.stdout
    assert "never awaited" not in out.stderr


async def test_concurrent_start_and_shutdown_during_start() -> None:
    s = cronoq.Scheduler([cronoq.Pool("default", echo, max=2)])
    first = s.start()
    # The build runs on a runtime thread and may already have finished (fast release builds),
    # so the second call reports either state; both are refusals.
    with pytest.raises(RuntimeError, match="already start(ing|ed)"):
        s.start()
    assert await first is None
    with pytest.raises(RuntimeError, match="already started"):
        s.start()
    await s.shutdown(1)

    s2 = cronoq.Scheduler([cronoq.Pool("default", echo, max=2)])
    # Shutting down right after start(): usually this cancels the pending start (which then
    # raises SchedulerError), but in fast builds the engine may already be built, in which
    # case shutdown stops it normally. Either way the scheduler ends up stopped.
    starting = asyncio.ensure_future(s2.start())
    await s2.shutdown(1)
    try:
        assert await starting is None
    except cronoq.SchedulerError:
        pass
    assert not s2.started
    # A stopped scheduler can be started again.
    await s2.start()
    assert await (await s2.submit(5)) == 5
    await s2.shutdown(1)


async def test_done_without_awaiting() -> None:
    async with cronoq.Scheduler([cronoq.Pool("default", echo)]) as s:
        h = await s.submit(1)
        for _ in range(200):
            if h.done():
                break
            await asyncio.sleep(0.01)
        assert h.done()
        assert await h == 1


def test_exceptions_pickle() -> None:
    for e in [
        cronoq.TaskFailed("failed", "boom", 3),
        cronoq.SubmitError("cycle", "dependency cycle"),
        cronoq.SchedulerError("x"),
        cronoq.Retryable("later", retry_after=2.0),
        cronoq.Permanent("never"),
    ]:
        back = pickle.loads(pickle.dumps(e))
        assert type(back) is type(e) and str(back) == str(e)
        assert back.__dict__ == e.__dict__
    assert issubclass(cronoq.Retryable, cronoq.CronoqError)
    assert issubclass(cronoq.Permanent, cronoq.CronoqError)


async def test_returns_none_not_empty_tuple() -> None:
    s = cronoq.Scheduler([cronoq.Pool("default", echo)])
    assert await s.start() is None
    assert await s.set_capacity("default", 2) is None
    assert await s.add_recurring("r", "0 0 1 1 *", 1) is None
    assert await s.shutdown(1) is None


async def test_default_pool_is_the_only_pool() -> None:
    async with cronoq.Scheduler([cronoq.Pool("workers", echo)]) as s:
        assert await (await s.submit(7)) == 7
        assert await s.enqueue(8) > 0
        hs = await s.submit_batch([cronoq.Task(9)])
        assert await hs[0] == 9


async def test_context_exposes_deadline() -> None:
    async def run(ctx: Any, p: Any) -> Any:
        return ctx.deadline

    async with cronoq.Scheduler([cronoq.Pool("default", run)]) as s:
        before = time.time()
        deadline = await (await s.submit(1, deadline_in=60.0))
        assert before + 59 < deadline < time.time() + 61
        assert await (await s.submit(2)) is None


async def test_user_cancelled_error_is_retryable() -> None:
    attempts: List[int] = []

    async def run(ctx: Any, p: Any) -> Any:
        attempts.append(ctx.attempt)
        if ctx.attempt == 1:
            fut = asyncio.get_running_loop().create_future()
            fut.cancel()
            await fut  # raises CancelledError that the engine did not ask for
        return "ok"

    async with cronoq.Scheduler([cronoq.Pool("default", run)]) as s:
        assert await (await s.submit(1, retry=cronoq.RetryPolicy(initial_backoff=0.01, jitter=False))) == "ok"
    assert attempts == [1, 2]


async def test_shutdown_grace_is_configurable() -> None:
    async def slow(ctx: Any, p: Any) -> Any:
        await asyncio.sleep(0.3)
        return p

    s = cronoq.Scheduler([cronoq.Pool("default", slow)], shutdown_grace=0.0)
    async with s:
        h = await s.submit(1)
        await asyncio.sleep(0.05)
    with pytest.raises(cronoq.TaskFailed) as e:
        await h
    assert e.value.status == "shutdown"


# ---------------------------------------------------------------------------------------------
# Events


async def test_events_terminal_only_by_default() -> None:
    async with cronoq.Scheduler([cronoq.Pool("default", echo)]) as s:
        stream = s.events()
        h = await s.submit(1, key="k")
        await h
        e = await asyncio.wait_for(stream.__anext__(), 5)
        assert (e.id, e.pool, e.key, e.status, e.attempt) == (h.id, "default", "k", "succeeded", 1)
        assert abs(e.at - time.time()) < 5 and e.message is None
        stream.close()
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()


async def test_events_all_transitions_and_stop_on_shutdown() -> None:
    s = cronoq.Scheduler([cronoq.Pool("default", echo)], emit_transitions=True)
    await s.start()
    seen: List[str] = []

    async def consume() -> None:
        async for e in s.events():
            seen.append(e.status)

    consumer = asyncio.ensure_future(consume())
    await asyncio.sleep(0.05)
    await (await s.submit(1))
    await asyncio.sleep(0.05)
    await s.shutdown(1)
    await asyncio.wait_for(consumer, 5)  # iteration ends when the scheduler stops
    assert seen == ["blocked", "ready", "running", "succeeded"]


async def test_events_lag_is_reported() -> None:
    async with cronoq.Scheduler([cronoq.Pool("default", echo, max=8)], event_capacity=4) as s:
        skip = s.events()
        strict = s.events(on_lag="raise")
        hs = [await s.submit(i) for i in range(50)]
        for h in hs:
            await h
        got = []
        for _ in range(4):
            got.append(await asyncio.wait_for(skip.__anext__(), 5))
        assert skip.lagged > 0 and len(got) == 4
        with pytest.raises(cronoq.EventsLagged) as e:
            await strict.__anext__()
        assert e.value.skipped > 0
        # The stream keeps going after a lag error.
        assert (await asyncio.wait_for(strict.__anext__(), 5)).status == "succeeded"


# ---------------------------------------------------------------------------------------------
# Compat shim


def test_compat_resubmitting_a_completed_id_runs_again() -> None:
    s = ElasticPriorityScheduler(initial_workers=2, max_workers=4)
    s.start()
    try:
        assert s.submit(CompatTask(id="a", duration=0.05))
        _wait(lambda: s.completed_count == 1)
        assert s.submit(CompatTask(id="a", duration=0.05))
        _wait(lambda: s.completed_count == 2)
        assert s.pending_tasks == []
    finally:
        s.stop()


def test_compat_stop_lets_running_tasks_finish_and_keeps_unfinished_ones() -> None:
    s = ElasticPriorityScheduler(initial_workers=1, max_workers=1)
    s.start()
    s.submit(CompatTask(id="long", duration=0.8))
    s.submit(CompatTask(id="queued", duration=0.1))
    time.sleep(0.2)
    s.stop()  # 2 s grace: "long" finishes, "queued" never started
    assert [c.task.id for c in s.completed_tasks] == ["long"]
    assert [t.id for t in s.pending_tasks] == ["queued"]
    s.start()  # unfinished work resumes after a restart
    try:
        _wait(lambda: s.completed_count == 2)
        assert s.get_metrics().schedule_quality <= 1.0
    finally:
        s.stop()


def test_compat_rejects_before_start_like_the_prototype() -> None:
    s = ElasticPriorityScheduler(max_workers=2)
    assert not s.submit(CompatTask(id="x", duration=0.1, dependencies=["x"]))
    assert s.submit(CompatTask(id="a", duration=0.1, dependencies=["b"]))
    assert not s.submit(CompatTask(id="b", duration=0.1, dependencies=["a"]))
    t = time.time() + 30
    assert s.submit(CompatTask(id="f1", duration=5, fixed_start=t))
    assert not s.submit(CompatTask(id="f2", duration=5, fixed_start=t + 1))
    assert sorted(t.id for t in s.pending_tasks) == ["a", "f1"]


def test_compat_stop_closes_its_loop() -> None:
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        for _ in range(3):
            s = ElasticPriorityScheduler()
            s.start()
            loop = s._loop
            s.stop()
            assert loop is not None and loop.is_closed()


def _wait(cond: Any, timeout: float = 10.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")
