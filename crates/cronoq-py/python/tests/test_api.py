"""Behavioural tests for the asyncio API."""

import asyncio
import time

import cronoq
import pytest
from cronoq import Dep, Pool, Priority, Resource, RetryPolicy, Scheduler, Task


async def echo(ctx, payload):
    return payload


def pool(executor=echo, **kw):
    kw.setdefault("max", 4)
    return Pool("default", executor, **kw)


async def test_submit_returns_result_and_handle_is_reawaitable():
    async with Scheduler([pool()]) as s:
        h = await s.submit({"x": 1})
        assert await h == {"x": 1}
        assert await h == {"x": 1}
        assert h.done()
        assert isinstance(h.id, int) and not h.duplicate
        assert await s.status(h.id) == "succeeded"


async def test_sync_executors_run_in_threads():
    def blocking(ctx, payload):
        time.sleep(0.05)
        return payload * 3

    async with Scheduler([pool(blocking)]) as s:
        handles = [await s.submit(i) for i in range(4)]
        t0 = time.monotonic()
        assert [await h for h in handles] == [0, 3, 6, 9]
        assert time.monotonic() - t0 < 1.0


async def test_priority_order_on_single_worker():
    order = []
    gate = asyncio.Event()

    async def run(ctx, payload):
        if payload == "blocker":
            await gate.wait()
        order.append(payload)

    async with Scheduler([Pool("default", run, max=1, autoscale=False)]) as s:
        hs = [await s.submit("blocker")]
        for name, prio in [("low", Priority.LOW), ("normal", "normal"), ("critical", 0), ("high", Priority.HIGH)]:
            hs.append(await s.submit(name, priority=prio))
        gate.set()
        for h in hs:
            await h
    assert order == ["blocker", "critical", "high", "normal", "low"]


async def test_retries_dead_letter_and_permanent_failures():
    calls = {}

    async def run(ctx, payload):
        calls[payload] = calls.get(payload, 0) + 1
        if payload == "flaky" and ctx.attempt < 3:
            raise RuntimeError("transient")
        if payload == "always":
            raise cronoq.Retryable("still broken")
        if payload == "bad":
            raise cronoq.Permanent("invalid input")
        return ctx.attempt

    fast = RetryPolicy(max_attempts=3, initial_backoff=0.01, max_backoff=0.05)
    async with Scheduler([pool(run, retry=fast)]) as s:
        assert await (await s.submit("flaky")) == 3
        with pytest.raises(cronoq.TaskFailed) as e:
            await (await s.submit("always"))
        assert e.value.status == "dead_lettered" and "still broken" in e.value.message
        with pytest.raises(cronoq.TaskFailed) as e:
            await (await s.submit("bad"))
        assert e.value.status == "failed"
    assert calls == {"flaky": 3, "always": 3, "bad": 1}


async def test_retry_after_is_honoured():
    starts = []

    async def run(ctx, payload):
        starts.append(time.monotonic())
        if ctx.attempt == 1:
            raise cronoq.Retryable("rate limited", retry_after=0.3)
        return "ok"

    async with Scheduler([pool(run)]) as s:
        assert await (await s.submit(1)) == "ok"
    assert starts[1] - starts[0] >= 0.29


async def test_timeouts_cancel_the_coroutine():
    cancelled = asyncio.Event()

    async def run(ctx, payload):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with Scheduler([pool(run)]) as s:
        h = await s.submit(1, timeout=0.1, retry=RetryPolicy.none())
        with pytest.raises(cronoq.TaskFailed) as e:
            await h
        assert e.value.status == "dead_lettered"
        await asyncio.wait_for(cancelled.wait(), 2)


async def test_cancellation_of_running_and_queued_tasks():
    started = asyncio.Event()

    async def run(ctx, payload):
        started.set()
        await asyncio.sleep(30)

    async with Scheduler([Pool("default", run, max=1, autoscale=False)]) as s:
        running = await s.submit("a", key="long")
        queued = await s.submit("b")
        await started.wait()
        queued.cancel()
        assert await s.cancel("long") is True
        for h in (running, queued):
            with pytest.raises(cronoq.TaskFailed) as e:
                await h
            assert e.value.status == "cancelled"
        assert await s.cancel("long") is False


async def test_dag_with_keys_batch_and_conditions():
    order = []

    async def run(ctx, payload):
        await asyncio.sleep(0.01)
        if payload == "fail":
            raise cronoq.Permanent("x")
        order.append(payload)

    async with Scheduler([pool(run)]) as s:
        hs = await s.submit_batch(
            [
                Task("c", key="c", after=["a", "b"]),
                Task("a", key="a"),
                Task("b", key="b", after="a"),
            ]
        )
        for h in hs:
            await h
        assert order == ["a", "b", "c"]

        f = await s.submit("fail", key="f")
        child = await s.submit("child", after="f")
        handler = await s.submit("handler", deps=[Dep("f", on="failure")])
        with pytest.raises(cronoq.TaskFailed):
            await f
        with pytest.raises(cronoq.TaskFailed) as e:
            await child
        assert e.value.status == "skipped"
        await handler
        assert "handler" in order

        dup = await s.submit("again", key="a")
        assert dup.duplicate

        with pytest.raises(cronoq.SubmitError) as e:
            await s.submit("x", after="does-not-exist")
        assert e.value.code == "unknown_dependency"
        with pytest.raises(cronoq.SubmitError) as e:
            await s.submit_batch([Task(1, key="x", after="y"), Task(2, key="y", after="x")])
        assert e.value.code == "cycle"


async def test_fixed_start_and_conflicts():
    started = {}

    async def run(ctx, payload):
        started[payload] = time.time()

    async with Scheduler([Pool("default", run, max=2, autoscale=False)]) as s:
        at = time.time() + 0.5
        h = await s.submit("fixed", start_at=at, duration_hint=1.0)
        with pytest.raises(cronoq.SubmitError) as e:
            await s.submit("clash", start_in=0.6, duration_hint=1.0)
        assert e.value.code == "reservation_conflict"
        await h
        assert abs(started["fixed"] - at) < 0.2


async def test_recurring_schedule():
    ticks = []

    async def run(ctx, payload):
        ticks.append(ctx.tags.get("cronoq.recurring"))

    async with Scheduler([pool(run)]) as s:
        await s.add_recurring("every-second", "* * * * * *", "tick")
        await asyncio.sleep(2.6)
        assert await s.remove_recurring("every-second")
        with pytest.raises(cronoq.SchedulerError):
            await s.add_recurring("bad", "not cron", "x")
    assert 2 <= len(ticks) <= 4
    assert set(ticks) == {"every-second"}


async def test_queued_recurring_occurrences_have_keys_and_deadlines():
    seen = []

    async def run(ctx, payload):
        seen.append((ctx.key, ctx.deadline))
        await asyncio.sleep(1.5)  # longer than the interval: occurrences queue up

    async with Scheduler([pool(run)]) as s:
        await s.add_recurring("q", "* * * * * *", "tick", overlap="queue", key="q", deadline_in=30)
        await asyncio.sleep(3.6)
        assert await s.remove_recurring("q")
        snap = await s.snapshot()
        assert snap.totals["skipped"] == 0
        with pytest.raises(ValueError, match="queue_on_success"):
            await s.add_recurring("bad", "* * * * * *", "x", overlap="sometimes")
    assert len(seen) >= 2
    for key, deadline in seen:
        assert key.startswith("q@") and key.endswith("Z")
        assert deadline is not None


async def test_backpressure_wait_reject_and_shed():
    async def slow(ctx, payload):
        await asyncio.sleep(0.05)
        return payload

    async with Scheduler([Pool("default", slow, max=1, autoscale=False, max_pending=2)]) as s:
        results = await asyncio.gather(*[(await s.submit(i)) for i in range(10)])
        assert results == list(range(10))

    async def forever(ctx, payload):
        await asyncio.sleep(30)

    async with Scheduler([Pool("default", forever, max=1, autoscale=False, max_pending=1, overflow="reject")]) as s:
        await s.enqueue(1)
        await s.enqueue(2)
        with pytest.raises(cronoq.SubmitError) as e:
            await s.enqueue(3)
        assert e.value.code == "overloaded"

    async with Scheduler([Pool("default", forever, max=1, autoscale=False, max_pending=1, overflow="shed_lowest")]) as s:
        await s.enqueue(1)
        low = await s.submit(2, priority="low")
        await s.submit(3, priority="high")
        with pytest.raises(cronoq.TaskFailed) as e:
            await low
        assert e.value.status == "shed"


async def test_resources_rate_limit():
    async with Scheduler([pool(max=8, autoscale=False)], resources=[Resource.token_bucket("api", 20.0, 1)]) as s:
        t0 = time.monotonic()
        hs = [await s.submit(i, resources={"api": 1}) for i in range(10)]
        for h in hs:
            await h
        assert 0.4 <= time.monotonic() - t0 < 1.5


async def test_context_fields_and_heartbeat():
    seen = {}

    async def run(ctx, payload):
        ctx.heartbeat()
        seen.update(id=ctx.id, attempt=ctx.attempt, kind=ctx.kind, key=ctx.key, pool=ctx.pool, tags=ctx.tags)
        assert not ctx.is_cancelled()
        return repr(ctx)

    async with Scheduler([pool(run, lease_ttl=5.0)]) as s:
        h = await s.submit(None, kind="llm", key="k1", tags={"tenant": "acme"})
        assert "TaskContext" in await h
    assert seen["kind"] == "llm" and seen["key"] == "k1" and seen["pool"] == "default"
    assert seen["tags"]["tenant"] == "acme" and seen["attempt"] == 1


async def test_snapshot_metrics():
    async def run(ctx, payload):
        await asyncio.sleep(0.01)

    async with Scheduler([pool(run)]) as s:
        for h in [await s.submit(i) for i in range(10)]:
            await h
        snap = await s.snapshot()
        p = snap.pool("default")
        assert p is not None and p.succeeded == 10
        assert p.priority_adherence == 1.0
        assert p.fixed_start_hit_rate == 1.0
        assert 0.0 < p.utilization <= 1.0
        assert p.wait_p99 >= p.wait_p50 >= 0.0
        assert snap.totals["succeeded"] == 10
        assert snap.pool("nope") is None


async def test_deadlines_and_option_validation():
    async with Scheduler([pool()]) as s:
        assert await (await s.submit(1, deadline_in=1.0)) == 1
        import datetime

        assert await (await s.submit(2, deadline_at=datetime.datetime.now() + datetime.timedelta(seconds=5))) == 2
        with pytest.raises(ValueError):
            await s.submit(1, priority="urgent")
        with pytest.raises(ValueError):
            await s.submit(1, start_in=1, start_at=time.time())
        with pytest.raises(cronoq.SubmitError) as e:
            await s.submit(1, pool="missing")
        assert e.value.code == "unknown_pool"


async def test_lifecycle_errors():
    s = Scheduler([pool()])
    with pytest.raises(RuntimeError):
        await s.submit(1)
    await s.start()
    with pytest.raises(RuntimeError):
        await s.start()
    pending_h = await s.submit(1)
    await pending_h
    await s.shutdown()
    assert not s.started
    with pytest.raises(RuntimeError):
        await s.submit(1)
    with pytest.raises(ValueError):
        Scheduler([])


async def test_tasks_with_labels_run_in_matching_groups():
    groups = []

    async def run(ctx, payload):
        groups.append((payload, dict(ctx.group)))

    async with Scheduler([pool(run, max=1, autoscale=False)]) as s:
        await s.set_group_capacity("default", {"gpu": "a100"}, 1)
        await (await s.submit("gpu", labels={"gpu": "a100"}))
        await (await s.submit("plain"))
    assert ("gpu", {"gpu": "a100"}) in groups
    assert ("plain", {}) in groups
