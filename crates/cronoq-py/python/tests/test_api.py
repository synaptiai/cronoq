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


async def test_update_moves_a_queued_task_and_spares_a_running_one():
    order: list[str] = []
    gate = asyncio.Event()

    async def run(ctx, payload):
        if payload == "first":
            await gate.wait()
        order.append(payload)

    async with Scheduler([Pool("default", run, max=1, autoscale=False)], emit_transitions=True) as s:
        events = s.events()
        first = await s.submit("first", key="first")
        while await s.status("first") != "running":
            await asyncio.sleep(0.01)
        a = await s.submit("a", key="a", priority="low")
        b = await s.submit("b", key="b", priority="low")

        r = await s.update("b", priority="high", deadline_in=30, tags={"why": "urgent"})
        assert (r.id, r.status, r.deferred, r.changed) == (b.id, "ready", False, True)
        assert "Updated(" in repr(r)
        assert not (await s.update(b.id, priority="high")).changed

        running = await s.update("first", priority="low")
        assert running.deferred and running.status == "running"

        with pytest.raises(cronoq.UpdateError) as missing:
            await s.update("nope", priority="low")
        assert missing.value.code == "not_found"
        with pytest.raises(ValueError):
            await s.update("b", deadline_in=1, clear_deadline=True)
        await s.submit("fixed", key="fixed", start_in=3600)
        with pytest.raises(cronoq.UpdateError) as refused:
            await s.update("fixed", tags={})
        assert refused.value.code == "fixed_start"
        assert await s.cancel("fixed")

        gate.set()
        await asyncio.gather(first.result(), a.result(), b.result())
        assert order == ["first", "b", "a"]

        seen = []
        async for e in events:
            if e.kind == "updated":
                seen.append((e.id, e.priority, e.status))
            if e.id == a.id and e.status == "succeeded":
                break
        assert (b.id, "high", "ready") in seen
        with pytest.raises(cronoq.UpdateError):
            await s.update("b", priority="low")


async def test_replace_by_key_latest_wins():
    seen: list[str] = []
    gate = asyncio.Event()

    async def run(ctx, payload):
        if payload == "blocker":
            await gate.wait()
        seen.append(payload)
        return payload

    async with Scheduler([Pool("default", run, max=1, autoscale=False)], emit_transitions=True) as s:
        events = s.events()
        blocker = await s.submit("blocker", key="b")
        while await s.status("b") != "running":
            await asyncio.sleep(0.01)
        first = await s.submit("v1", key="title")
        assert first.outcome == "created"
        # Not started: replaced in place, same task.
        for v in ("v2", "v3"):
            h = await s.submit(v, key="title", on_key_conflict="replace_queued")
            assert (h.id, h.duplicate, h.outcome) == (first.id, True, "replaced")
        assert (await s.submit("ignored", key="title")).outcome == "existing"
        # Started: the payload stays, and the newest submission runs after it.
        stored = await s.submit("b2", key="b", on_key_conflict="replace_queued", renew_position=True)
        assert (stored.id, stored.outcome) == (blocker.id, "pending")
        assert await s.enqueue("b3", key="b", on_key_conflict="replace_queued") == blocker.id

        with pytest.raises(ValueError):
            await s.submit("x", key="title", renew_position=True)
        with pytest.raises(ValueError):
            await s.submit("x", key="title", on_key_conflict="newest")
        with pytest.raises(cronoq.SubmitError) as mismatch:
            await s.submit("x", key="title", labels={"gpu": "a"}, on_key_conflict="replace_queued")
        assert mismatch.value.code == "replace_mismatch"
        with pytest.raises(cronoq.SubmitError):
            await s.submit("x", on_key_conflict="replace_queued", key="title", start_in=60)

        gate.set()
        assert await blocker == "blocker"
        assert await first == "v3"
        successor = await s.handle("b")
        assert successor is not None and successor.id != blocker.id
        assert await successor == "b3"
        assert seen == ["blocker", "b3", "v3"]

        kinds = []
        async for e in events:
            if e.kind != "transition":
                kinds.append((e.kind, e.id))
            if e.id == first.id and e.status == "succeeded":
                break
        assert kinds.count(("replaced", first.id)) == 2
        assert kinds.count(("replaced", blocker.id)) == 2
        assert ("successor", successor.id) in kinds


async def test_key_conflicts_are_reported_by_fingerprint():
    gate = asyncio.Event()

    async def run(ctx, payload):
        await gate.wait()
        return payload

    digest = bytes(range(16))
    async with Scheduler([Pool("default", run, max=1, autoscale=False)]) as s:
        first = await s.submit("edit-1", key="op-7", fingerprint=digest)
        # The same fingerprint (as bytes or as the int they spell) is a retry.
        same = await s.submit(
            "edit-1", key="op-7", fingerprint=int.from_bytes(digest, "big"), on_key_conflict="reject"
        )
        assert (same.id, same.outcome) == (first.id, "existing")
        with pytest.raises(cronoq.SubmitError) as e:
            await s.submit("edit-2", key="op-7", fingerprint=7, on_key_conflict="reject")
        err = e.value
        assert (err.code, err.task_id, err.index) == ("key_conflict", first.id, None)
        assert err.task_status in ("ready", "running")
        # Without the option, or without a fingerprint on one side, it is a duplicate.
        assert (await s.submit("edit-2", key="op-7", fingerprint=7)).id == first.id
        assert (await s.submit("edit-2", key="op-7", on_key_conflict="reject")).id == first.id

        batch = [
            Task("new", key="op-8", fingerprint=1, on_key_conflict="reject"),
            Task("edit-2", key="op-7", fingerprint=7, on_key_conflict="reject"),
        ]
        with pytest.raises(cronoq.SubmitError) as b:
            await s.submit_batch(batch)
        assert (b.value.code, b.value.index, b.value.task_id) == ("key_conflict", 1, first.id)
        assert await s.status("op-8") is None, "nothing of the batch was admitted"

        with pytest.raises(ValueError):
            await s.submit("x", key="k", fingerprint=b"short")
        with pytest.raises(ValueError):
            await s.submit("x", key="k", fingerprint=-1)
        with pytest.raises(ValueError):
            Task("x", key="k", on_key_conflict="replace_queued")
        gate.set()
        assert await first == "edit-1"

    # The scheduler's default can be to report conflicts; a submission can still opt out.
    async with Scheduler([Pool("default", run, max=1, autoscale=False)], on_key_conflict="reject") as s:
        first = await s.submit("a", key="k", fingerprint=1)
        with pytest.raises(cronoq.SubmitError):
            await s.submit("b", key="k", fingerprint=2)
        assert (await s.submit("b", key="k", fingerprint=2, on_key_conflict="return_existing")).id == first.id
    with pytest.raises(ValueError):
        Scheduler([Pool("default", run)], on_key_conflict="replace_queued")


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


def test_priority_and_urgency_config_validate_on_construction():
    d = cronoq.PriorityConfig()
    assert d.base == [10000.0, 1000.0, 100.0, 10.0]
    assert d.aging_rate == [2.0] * 4 and d.age_cap == [800.0] * 4
    assert d.allow_overtaking_high is False
    u = cronoq.UrgencyConfig()
    assert (u.max, u.window, u.overdue_rate, u.overdue_cap) == (5000.0, 5.0, 100.0, 3000.0)
    assert "PriorityConfig(" in repr(d) and "UrgencyConfig(" in repr(u)

    with pytest.raises(ValueError, match="low work overtake fresh high"):
        cronoq.PriorityConfig(age_cap=(800, 800, 800, 5000))
    ok = cronoq.PriorityConfig(age_cap=(800, 800, 800, 5000), allow_overtaking_high=True)
    assert ok.age_cap == [800.0, 800.0, 800.0, 5000.0]
    with pytest.raises(ValueError, match="strictly ordered"):
        cronoq.PriorityConfig(base=(10, 10, 5, 1))
    with pytest.raises(ValueError, match="finite"):
        cronoq.PriorityConfig(aging_rate=(2, 2, -1, 2))
    with pytest.raises((TypeError, ValueError)):
        cronoq.PriorityConfig(base=(3, 2, 1))
    with pytest.raises(ValueError, match="urgency"):
        cronoq.UrgencyConfig(max=-1)
    with pytest.raises(ValueError, match="window"):
        cronoq.UrgencyConfig(window=float("nan"))


async def test_priority_config_applies_per_scheduler_and_per_pool():
    release = asyncio.Event()
    order: list[tuple[str, str]] = []

    def runner(name):
        async def run(ctx, payload):
            if payload == "block":
                await release.wait()
            else:
                order.append((name, payload))

        return run

    # Aging so fast that a low task passes fresh high work after about 10 ms of waiting.
    fast = cronoq.PriorityConfig(
        aging_rate=(0, 0, 0, 100_000), age_cap=(0, 0, 0, 5000), allow_overtaking_high=True
    )
    pools = [
        Pool("default", runner("default"), max=1, autoscale=False),
        Pool("tuned", runner("tuned"), max=1, autoscale=False, priority=fast),
    ]
    async with Scheduler(pools, urgency=cronoq.UrgencyConfig(max=7000.0)) as s:
        handles = []
        for p in ("default", "tuned"):
            handles.append(await s.submit("block", pool=p))
        await asyncio.sleep(0.05)
        for p in ("default", "tuned"):
            handles.append(await s.submit("low", pool=p, priority="low"))
        await asyncio.sleep(0.3)
        for p in ("default", "tuned"):
            handles.append(await s.submit("high", pool=p, priority="high"))
        release.set()
        for h in handles:
            await h
        snap = await s.snapshot()
        default, tuned = snap.pool("default"), snap.pool("tuned")
        assert default.priority.age_cap == [800.0] * 4
        assert tuned.priority.age_cap == [0.0, 0.0, 0.0, 5000.0]
        assert tuned.priority.allow_overtaking_high is True
        # The scheduler-wide urgency reaches every pool without its own.
        assert default.urgency.max == 7000.0 and tuned.urgency.max == 7000.0
    assert [x for x in order if x[0] == "default"] == [("default", "high"), ("default", "low")]
    assert [x for x in order if x[0] == "tuned"] == [("tuned", "low"), ("tuned", "high")]


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


async def test_fair_pools_share_by_tenant_weight():
    order = []
    queued = asyncio.Event()

    async def run(ctx, payload):
        await queued.wait()  # the first task holds the only slot until everything is queued
        order.append(payload)

    p = pool(run, max=1, autoscale=False, tenant_weights={"acme": 3.0})
    async with Scheduler([p]) as s:
        handles = [await s.submit("globex", tenant="globex") for _ in range(20)]
        handles += [await s.submit("acme", tenant="acme") for _ in range(20)]
        queued.set()
        for h in handles:
            await h
    first = order[:20]
    assert 13 <= first.count("acme") <= 16, first
    with pytest.raises(ValueError, match="tenant weights"):
        Pool("x", run, tenant_weights={"acme": 0.0})


async def test_ordering_keys_run_one_at_a_time_in_submission_order():
    running: dict[int, int] = {}
    overlap = []
    order = []

    async def run(ctx, payload):
        key, n = payload
        running[key] = running.get(key, 0) + 1
        overlap.append(running[key])
        order.append((key, n))
        await asyncio.sleep(0.005)
        running[key] -= 1

    async with Scheduler([pool(run, max=8)]) as s:
        handles = []
        for n in range(3):
            for key in range(20):
                # Priorities do not reorder a key: the low task submitted first runs first.
                priority = "low" if n == 0 else "critical"
                handles.append(await s.submit((key, n), ordering_key=f"k{key}", priority=priority))
        assert (await s.snapshot()).pool("default").ordering_keys == 20
        for h in handles:
            await h
        # An ordering key is not a deduplication key: these are two tasks, in a batch too.
        batch = await s.submit_batch([Task((99, n), ordering_key="batch") for n in range(3)])
        assert len({h.id for h in batch}) == 3
        for h in batch:
            await h
    assert max(overlap) == 1
    for key in [*range(20), 99]:
        assert [n for k, n in order if k == key] == [0, 1, 2]


async def test_ordering_key_halts_on_failure_and_is_resumed():
    async def run(ctx, payload):
        if payload == "bad":
            raise cronoq.Permanent("boom")
        return payload

    async with Scheduler([pool(run)]) as s:
        bad = await s.submit("bad", ordering_key="acct", halt_on_failure=True)
        nxt = await s.submit("next", ordering_key="acct", priority="low")
        with pytest.raises(cronoq.TaskFailed):
            await bad
        await asyncio.sleep(0.1)
        info = await s.ordering("acct")
        assert info.halted and (info.tasks, info.head) == (1, nxt.id)
        assert await s.status(nxt.id) == "ready"
        assert await s.halted_orderings() == ["acct"]
        snap = (await s.snapshot()).pool("default")
        assert (snap.ordering_halted, snap.ordering_waiting) == (1, 1)

        # A floor changes the class the head runs at, not the task's own.
        await s.set_ordering_floor("acct", Priority.CRITICAL)
        info = await s.ordering("acct")
        assert (info.floor, info.effective, info.persist) == ("critical", "critical", False)
        await s.set_ordering_floor("acct", None)
        assert (await s.ordering("acct")).effective == "low"

        assert await s.resume_ordering("acct") is True
        assert await s.resume_ordering("acct") is False
        assert await nxt == "next"
        assert await s.ordering("acct") is None

        # A floor that is kept outlives the key's tasks.
        await s.set_ordering_floor("vip", "high", persist=True)
        info = await s.ordering("vip")
        assert (info.tasks, info.floor, info.persist) == (0, "high", True)

        with pytest.raises(ValueError, match="ordering_key"):
            await s.submit("x", halt_on_failure=True)
        with pytest.raises(ValueError, match="ordering_key"):
            Task("x", halt_on_failure=True)
        with pytest.raises(ValueError, match="floor"):
            await s.set_ordering_floor("k", None, persist=True)
        with pytest.raises(cronoq.SubmitError):
            await s.submit("x", ordering_key="k", start_in=60)
        with pytest.raises(cronoq.SubmitError):
            await s.ordering("k", pool="missing")
