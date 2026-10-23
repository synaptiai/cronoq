import assert from "node:assert/strict";
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
