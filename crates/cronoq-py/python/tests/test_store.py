"""Durable schedulers: state in a SQLite file survives restarts."""

import asyncio
import pickle

import cronoq
import pytest
from cronoq import Pool, Scheduler, SqliteStore


def store(tmp_path, **kw):
    kw.setdefault("lease_ttl", 1.5)
    kw.setdefault("lease_renew", 0.2)
    kw.setdefault("lock_poll", 0.05)
    return SqliteStore(tmp_path / "state.db", **kw)


async def double(ctx, payload):
    return {"n": payload["n"] * 2}


async def test_queued_work_and_results_survive_a_restart(tmp_path):
    gate = asyncio.Event()

    async def stuck(ctx, payload):
        await gate.wait()
        return payload

    first = Scheduler([Pool("default", stuck, max=1, autoscale=False)], store=store(tmp_path))
    await first.start()
    for i in range(3):
        await first.enqueue({"n": i}, key=f"job-{i}")
    await first.shutdown(grace=0)

    async with Scheduler([Pool("default", double, max=2)], store=store(tmp_path)) as second:
        for i in range(3):
            h = await second.handle(f"job-{i}")
            assert h is not None
            assert await h == {"n": i * 2}

    # Finished results are decoded from the file after another restart.
    async with Scheduler([Pool("default", double, max=2)], store=store(tmp_path)) as third:
        assert await third.status("job-1") == "succeeded"
        h = await third.handle("job-1")
        assert h is not None and await h == {"n": 2}
        dup = await third.submit({"n": 99}, key="job-1")
        assert dup.duplicate


async def test_recurring_schedules_survive_a_restart(tmp_path):
    async with Scheduler([Pool("default", double)], store=store(tmp_path)) as s:
        await s.add_recurring("nightly", "0 3 * * *", {"n": 1})
    async with Scheduler([Pool("default", double)], store=store(tmp_path)) as s:
        assert await s.remove_recurring("nightly")


async def test_payloads_the_codec_cannot_encode_are_refused_at_submit(tmp_path):
    async with Scheduler([Pool("default", double)], store=store(tmp_path)) as s:
        for bad in (b"raw bytes", object(), {1, 2}):
            with pytest.raises(TypeError, match="cannot be stored"):
                await s.submit(bad)
        with pytest.raises(TypeError):
            await s.submit_batch([cronoq.Task({"n": 1}), cronoq.Task(object())])
        # Nothing of the refused batch was submitted.
        assert (await s.snapshot()).totals["submitted"] == 0


async def test_a_custom_codec_is_used_both_ways(tmp_path):
    class Pickle:
        def encode(self, obj):
            return pickle.dumps(obj)

        def decode(self, data):
            return pickle.loads(data)

    gate = asyncio.Event()

    async def stuck(ctx, payload):
        await gate.wait()

    async def echo(ctx, payload):
        return payload

    first = Scheduler([Pool("default", stuck, max=1, autoscale=False)], store=store(tmp_path), codec=Pickle())
    await first.start()
    await first.enqueue(b"\x00bytes", key="b")
    await first.shutdown(grace=0)
    async with Scheduler([Pool("default", echo)], store=store(tmp_path), codec=Pickle()) as s:
        h = await s.handle("b")
        assert h is not None and await h == b"\x00bytes"


async def test_a_standby_starts_when_the_leader_stops(tmp_path):
    leader = Scheduler([Pool("default", double)], store=store(tmp_path))
    await leader.start()
    standby = Scheduler([Pool("default", double)], store=store(tmp_path))
    starting = asyncio.ensure_future(standby.start())
    await asyncio.sleep(0.5)
    assert not starting.done(), "the standby must wait while the leader runs"
    await leader.shutdown()
    await asyncio.wait_for(starting, 5)
    h = await standby.submit({"n": 4})
    assert await h == {"n": 8}
    await standby.shutdown()


def test_store_and_codec_arguments_are_checked(tmp_path):
    p = [Pool("default", double)]
    with pytest.raises(ValueError, match="needs store"):
        Scheduler(p, codec=cronoq.JsonCodec())
    with pytest.raises(TypeError, match="encode"):
        Scheduler(p, store=store(tmp_path), codec=object())
    with pytest.raises(ValueError, match="lease_renew"):
        SqliteStore(tmp_path / "x.db", lease_ttl=1.0, lease_renew=2.0)
    assert "x.db" in repr(SqliteStore(tmp_path / "x.db"))


def test_json_codec_round_trips():
    c = cronoq.JsonCodec()
    v = {"a": [1, 2.5, None, True], "ü": "ñ"}
    assert c.decode(c.encode(v)) == v
