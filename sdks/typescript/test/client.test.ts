import assert from "node:assert/strict";
import { createServer } from "node:net";
import { after, afterEach, before, describe, test } from "node:test";

import { CancelTask, CronoqError, Permanent, Retryable, connect, runWorker, text, type Handler, type WorkerOptions } from "../src/node.js";
import { ADMIN, CLIENT, Server, WORKER, serverBinary } from "./server.js";

const bin = serverBinary();
const quiet = { info() {}, warn() {}, error() {} };

const workers: Array<() => Promise<void>> = [];

/** Runs a worker until the returned stop function is called (or the test ends). */
function worker(target: string, handler: Handler, opts: Partial<WorkerOptions> = {}) {
  const ac = new AbortController();
  const done = runWorker({ target, pool: "default", token: WORKER, logger: quiet, signal: ac.signal, ...opts }, handler);
  const stop = async () => {
    ac.abort();
    await done;
  };
  workers.push(stop);
  return stop;
}

const upper: Handler = (l) => l.text().toUpperCase();

async function waitFor(cond: () => Promise<boolean> | boolean, what: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

describe("against a real server", { skip: bin === undefined && "cronoq-server not built" }, () => {
  let s: Server;
  before(async () => {
    s = await Server.create(bin!);
    await s.start();
  });
  after(() => s.stop());
  afterEach(async () => {
    await Promise.all(workers.splice(0).map((stop) => stop()));
  });

  test("submit, wait and inspect", async () => {
    const stop = worker(s.target, upper, { slots: 2 });
    const c = connect(s.target, { token: CLIENT });
    const sub = await c.submitAndWait({ payload: "hello", key: "greeting" }, 10);
    assert.equal(sub.result?.status, "succeeded");
    assert.equal(text(sub.result?.output), "HELLO");
    // The key makes resubmission idempotent: the finished task and its output come back.
    const again = await c.submitAndWait({ payload: "other", key: "greeting" }, 10);
    assert.ok(again.duplicate);
    assert.equal(again.id, sub.id);
    assert.equal(text(again.result?.output), "HELLO");
    const info = await c.get("greeting");
    assert.equal(info.id, sub.id);
    assert.equal(info.status, "succeeded");
    const page = await c.listTasks({ statuses: ["succeeded"] });
    assert.ok(page.tasks.some((t) => t.id === sub.id));
    await assert.rejects(c.get(1n << 50n), (e: unknown) => e instanceof CronoqError && e.notFound);
    // Large ids survive: they are bigints.
    assert.equal(typeof sub.id, "bigint");
    await stop();
  });

  test("batches, waits, cancel and watch", async () => {
    const c = connect(s.target, { token: CLIENT });
    const ac = new AbortController();
    const seen = new Map<string, string>();
    const watching = (async () => {
      for await (const e of c.watch({ keys: ["first", "second"] }, { signal: ac.signal })) seen.set(e.key, e.status);
    })();
    await new Promise((r) => setTimeout(r, 200));
    const [first, second] = await c.submitBatch([
      { payload: "a", key: "first" },
      { payload: "b", key: "second", after: ["first"] },
    ]);
    // Nothing serves the pool: waiting times out and the task stays queued.
    await assert.rejects(c.wait(first!.id, 0.2), (e: unknown) => e instanceof CronoqError && e.timeout);
    assert.equal(await c.cancel("first"), true);
    const r = await c.wait(second!.id, 10);
    assert.notEqual(r.status, "succeeded");
    await waitFor(() => seen.size === 2, "two events");
    assert.equal(seen.get("first"), "cancelled");
    ac.abort();
    await watching;
  });

  test("update changes a queued task", async () => {
    const c = connect(s.target, { token: CLIENT });
    const ac = new AbortController();
    const events: Array<{ updated: boolean; priority?: string; status: string }> = [];
    const watching = (async () => {
      for await (const e of c.watch({ keys: ["upd"], allTransitions: true }, { signal: ac.signal })) events.push(e);
    })();
    await new Promise((r) => setTimeout(r, 200));
    // Nothing serves the pool, so the task stays queued.
    const sub = await c.submit({ payload: "x", key: "upd", priority: "low" });
    const u = await c.update("upd", { priority: "high", deadlineIn: 60, tags: { team: "search" } });
    assert.deepEqual(u, { id: sub.id, status: "ready", deferred: false, changed: true });
    assert.equal((await c.get(sub.id)).priority, "high");
    // The same class again changes nothing; clearing the deadline and the tags does.
    assert.equal((await c.update(sub.id, { priority: "high" })).changed, false);
    assert.equal((await c.update(sub.id, { clearDeadline: true, tags: {} })).changed, true);
    assert.equal((await c.update(sub.id, { deadlineAt: new Date(Date.now() + 3_600_000) })).changed, true);

    await assert.rejects(c.update("missing", { priority: "high" }), (e: unknown) => e instanceof CronoqError && e.code === "not_found");
    await assert.rejects(
      c.update(sub.id, { deadlineIn: 1, clearDeadline: true }),
      (e: unknown) => e instanceof CronoqError && e.code === "invalid_argument",
    );

    await waitFor(() => events.some((e) => e.updated), "an update event");
    const first = events.find((e) => e.updated)!;
    assert.equal(first.priority, "high");
    assert.equal(first.status, "ready");
    assert.ok(events.filter((e) => !e.updated && events.indexOf(e) < events.indexOf(first)).every((e) => e.priority === "low"));
    ac.abort();
    await watching;
  });

  test("replace by key: the newest submission wins", async () => {
    const c = connect(s.target, { token: CLIENT });
    // Nothing serves the pool yet: the first task stays queued and is replaced in place.
    const first = await c.submit({ payload: "v1", key: "title" });
    assert.equal(first.outcome, "created");
    const newer = await c.submit({ payload: "v2", key: "title", onKeyConflict: "replace_queued" });
    assert.deepEqual(
      { id: newer.id, duplicate: newer.duplicate, outcome: newer.outcome },
      { id: first.id, duplicate: true, outcome: "replaced" },
    );
    assert.equal((await c.submit({ payload: "v3", key: "title" })).outcome, "existing");
    await assert.rejects(
      c.submit({ payload: "x", key: "title", labels: { gpu: "a" }, onKeyConflict: "replace_queued" }),
      (e: unknown) => e instanceof CronoqError && e.code === "failed_precondition",
    );
    await assert.rejects(
      c.submit({ payload: "x", key: "title", renewPosition: true }),
      (e: unknown) => e instanceof CronoqError && e.code === "invalid_argument",
    );
    // A batch admits tasks as written, and a misspelt option is an error: neither is dropped.
    await assert.rejects(
      c.submitBatch([{ payload: "x", key: "title", onKeyConflict: "replace_queued" }]),
      (e: unknown) => e instanceof CronoqError && e.code === "invalid_argument" && !e.keyConflict,
    );
    await assert.rejects(
      c.submit({ payload: "x", key: "title", onKeyConflict: "newest" as never }),
      (e: unknown) => e instanceof CronoqError && e.code === "invalid_argument",
    );

    const ac = new AbortController();
    const events: Array<{ kind: string; id: bigint; message: string }> = [];
    const watching = (async () => {
      for await (const e of c.watch({ keys: ["title"], allTransitions: true }, { signal: ac.signal })) events.push(e);
    })();
    await new Promise((r) => setTimeout(r, 200));

    const seen: string[] = [];
    let release!: () => void;
    const released = new Promise<void>((r) => (release = r));
    // The server is shared with the other tests: finish whatever else is still queued.
    const stop = worker(s.target, async (l) => {
      if (l.key !== "title") return l.payload;
      seen.push(l.text());
      await released;
      return l.payload;
    });
    await waitFor(() => seen.length === 1, "the first attempt");
    // The task has started: its payload stays, and the newest submission follows it.
    for (const v of ["v4", "v5"]) {
      const stored = await c.submit({ payload: v, key: "title", onKeyConflict: "replace_queued", renewPosition: true });
      assert.deepEqual({ id: stored.id, outcome: stored.outcome }, { id: first.id, outcome: "pending" });
    }
    release();
    assert.equal(text((await c.wait(first.id, 10)).output), "v2");
    await waitFor(() => events.some((e) => e.kind === "successor"), "a successor event");
    const successor = events.find((e) => e.kind === "successor")!;
    assert.notEqual(successor.id, first.id);
    assert.equal(successor.message, `follows task ${first.id}`);
    assert.equal(events.filter((e) => e.kind === "replaced" && e.id === first.id).length, 2);
    assert.equal(text((await c.wait(successor.id, 10)).output), "v5");
    assert.deepEqual(seen, ["v2", "v5"]);
    ac.abort();
    await watching;
    await stop();
  });

  test("a key reused with other content is reported", async () => {
    const c = connect(s.target, { token: CLIENT });
    const first = await c.submit({ payload: "edit-1", key: "op-7" });
    // A retry of the same submission is answered with the task; other content is a conflict.
    const again = await c.submit({ payload: "edit-1", key: "op-7", onKeyConflict: "reject" });
    assert.deepEqual({ id: again.id, outcome: again.outcome }, { id: first.id, outcome: "existing" });
    await assert.rejects(c.submit({ payload: "edit-2", key: "op-7", onKeyConflict: "reject" }), (e: unknown) => {
      assert.ok(e instanceof CronoqError);
      assert.equal(e.code, "failed_precondition");
      assert.deepEqual(
        { conflict: e.keyConflict, task: e.conflictTask, status: e.conflictStatus, index: e.batchIndex },
        { conflict: true, task: first.id, status: "ready", index: undefined },
      );
      return true;
    });
    // Without the option the submission is still answered with the existing task.
    assert.equal((await c.submit({ payload: "edit-2", key: "op-7" })).id, first.id);

    const batch = [
      { payload: "new", key: "op-8", onKeyConflict: "reject" as const },
      { payload: "edit-2", key: "op-7", onKeyConflict: "reject" as const },
    ];
    await assert.rejects(c.submitBatch(batch), (e: unknown) => {
      assert.ok(e instanceof CronoqError && e.keyConflict);
      assert.deepEqual({ index: e.batchIndex, task: e.conflictTask }, { index: 1, task: first.id });
      return true;
    });
    // Nothing of the batch was admitted.
    await assert.rejects(c.get("op-8"), (e: unknown) => e instanceof CronoqError && e.code === "not_found");
    await assert.rejects(
      c.submitBatch([batch[0]!, { payload: "edit-2", key: "op-7" }]),
      (e: unknown) => e instanceof CronoqError && e.code === "invalid_argument",
    );
    await c.cancel("op-7");
  });

  test("handler outcomes and fencing tokens", async () => {
    const attempts = new Map<bigint, number[]>();
    const stop = worker(
      s.target,
      async (l) => {
        attempts.set(l.taskId, [...(attempts.get(l.taskId) ?? []), l.attempt]);
        switch (l.text()) {
          case "permanent":
            throw new Permanent("bad input");
          case "flaky":
            if (l.attempt === 1) throw new Retryable("busy", 0.05);
            return { attempt: l.attempt };
          case "cancel":
            throw new CancelTask("not needed");
          case "throw":
            throw new Error("boom");
        }
      },
      { slots: 4 },
    );
    const c = connect(s.target, { token: CLIENT });
    const want = { permanent: "failed", flaky: "succeeded", cancel: "cancelled", throw: "dead_lettered" };
    for (const [payload, status] of Object.entries(want)) {
      const sub = await c.submitAndWait({ payload }, 20);
      assert.equal(sub.result?.status, status, `${payload}: ${sub.result?.message}`);
      if (payload === "flaky") {
        // (taskId, attempt) is a fencing token: the retry has a higher attempt number.
        assert.deepEqual(attempts.get(sub.id), [1, 2]);
        assert.deepEqual(JSON.parse(text(sub.result?.output)), { attempt: 2 });
      }
    }
    await stop();
  });

  test("an aborted attempt aborts the handler's signal", async () => {
    let aborted = 0;
    const stop = worker(s.target, (l) =>
      new Promise((resolve, reject) => {
        const t = setTimeout(() => resolve("late"), 20_000);
        l.signal.addEventListener("abort", () => {
          clearTimeout(t);
          aborted++;
          reject(new Error("aborted"));
        });
      }),
    );
    const c = connect(s.target, { token: CLIENT });
    const sub = await c.submitAndWait({ timeout: 0.2, maxAttempts: 1 }, 15);
    assert.notEqual(sub.result?.status, "succeeded");
    await waitFor(() => aborted === 1, "the abort");
    await stop();
  });

  test("heartbeats keep long leases alive", async () => {
    const stop = worker(s.target, async (l) => {
      await new Promise((r) => setTimeout(r, 2500)); // lease_ttl_s = 1
      return String(l.attempt);
    });
    const sub = await connect(s.target, { token: CLIENT }).submitAndWait({}, 20);
    assert.equal(sub.result?.status, "succeeded");
    assert.equal(text(sub.result?.output), "1");
    await stop();
  });

  test("stopping drains running handlers", async () => {
    let started!: () => void;
    const running = new Promise<void>((r) => (started = r));
    const stop = worker(s.target, async (l) => {
      started();
      await new Promise((r) => setTimeout(r, 500));
      if (l.signal.aborted) throw new Error("cut short by the drain timeout");
      return "done";
    });
    const c = connect(s.target, { token: CLIENT });
    const sub = await c.submit({});
    await running;
    await stop();
    const r = await c.wait(sub.id, 5);
    assert.equal(r.status, "succeeded");
    assert.equal(text(r.output), "done");
  });

  test("a handler that ignores its signal does not block stopping", async () => {
    let started!: () => void;
    const running = new Promise<void>((r) => (started = r));
    const stop = worker(
      s.target,
      () => {
        started();
        return new Promise(() => {}); // never settles
      },
      { drainTimeout: 0.1 },
    );
    await connect(s.target, { token: CLIENT }).submit({});
    await running;
    const t0 = Date.now();
    await stop();
    assert.ok(Date.now() - t0 < 8000, `stopping took ${Date.now() - t0} ms`);
  });

  test("stopping works before the server answers", async () => {
    // Accepts connections and never answers.
    const sockets: Array<{ destroy(): void }> = [];
    const silent = createServer((socket) => void sockets.push(socket));
    await new Promise<void>((r) => silent.listen(0, "127.0.0.1", r));
    const port = (silent.address() as { port: number }).port;
    const ac = new AbortController();
    const done = runWorker({ target: `http://127.0.0.1:${port}`, pool: "default", token: WORKER, logger: quiet, signal: ac.signal }, upper);
    await new Promise((r) => setTimeout(r, 300));
    ac.abort();
    const t0 = Date.now();
    await done;
    assert.ok(Date.now() - t0 < 2000, `stopping took ${Date.now() - t0} ms`);
    for (const socket of sockets) socket.destroy();
    silent.close();
  });

  test("workers reconnect after a restart", async () => {
    const admin = connect(s.target, { token: ADMIN });
    const connected = async () => {
      try {
        return (await admin.snapshot()).workersConnected === 1n;
      } catch {
        return false;
      }
    };
    const stop = worker(s.target, upper, { initialBackoff: 0.05, maxBackoff: 0.2 });
    await waitFor(connected, "the worker to connect");
    await s.stop();
    await s.start();
    await waitFor(connected, "the worker to reconnect");
    const sub = await connect(s.target, { token: CLIENT }).submitAndWait({ payload: "back" }, 20);
    assert.equal(text(sub.result?.output), "BACK");
    await stop();
  });

  test("rejected workers stop", async () => {
    await assert.rejects(
      runWorker({ target: s.target, pool: "default", token: "wrong-token-0123456789", logger: quiet }, upper),
      (e: unknown) => e instanceof CronoqError && e.code === "unauthenticated",
    );
    await assert.rejects(
      runWorker({ target: s.target, pool: "nope", token: WORKER, logger: quiet }, upper),
      (e: unknown) => e instanceof CronoqError && e.notFound,
    );
    // TLS settings with plain http are refused rather than dropped.
    await assert.rejects(runWorker({ target: s.target, pool: "default", tls: {}, logger: quiet }, upper), /need an https/);
  });
});
