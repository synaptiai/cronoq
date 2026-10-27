import assert from "node:assert/strict";
import { test } from "node:test";

import { CronoqError, connect, runWorker, text } from "../src/node.js";
import { isLoopback, redirect } from "../src/worker.js";
import { CLIENT, Server, WORKER, freePort, serverBinary } from "./server.js";

const bin = serverBinary();
const quiet = { info() {}, warn() {}, error() {} };

function refusal(code: string, endpoint?: string): CronoqError {
  const md = new Headers();
  if (endpoint) md.set("cronoq-shard-endpoint", endpoint);
  return new CronoqError(code, "pool 'llm' is served by shard 'gpu'", md);
}

test("redirects never weaken the connection", () => {
  const fp = "failed_precondition";
  const secure = { token: "t" };
  assert.equal(redirect(secure, "https://cpu:7070", refusal(fp, "https://gpu:7070")), "https://gpu:7070");
  // Not a redirect: another code, or no endpoint.
  assert.equal(redirect(secure, "https://cpu:7070", refusal("not_found", "https://gpu:7070")), undefined);
  assert.equal(redirect(secure, "https://cpu:7070", refusal(fp)), undefined);
  // No downgrade from https, not even to loopback; no other schemes, userinfo or paths.
  for (const bad of ["http://127.0.0.1:7070", "unix:/tmp/s", "https://u:p@gpu:7070", "https://gpu:7070/x", "https://gpu[::1]:7070"]) {
    assert.throws(() => redirect(secure, "https://cpu:7070", refusal(fp, bad)), CronoqError, bad);
  }
  // From plain http with a token: loopback and https are fine, another plain-http host is not.
  const ok = (t: string) => redirect({ token: "t" }, "http://127.0.0.1:7070", refusal(fp, t));
  assert.ok(ok("http://127.0.0.1:7071"));
  assert.ok(ok("http://[::1]:7071"));
  assert.ok(ok("http://localhost:7071"));
  assert.ok(ok("https://gpu.internal:7070"));
  assert.throws(() => ok("http://gpu.internal:7070"), /insecureRedirectHttp/);
  assert.throws(() => ok("http://127.0.0.1:1@gpu.internal:7070"));
  // Without a token there is nothing to leak; with the opt-in, plain http is allowed.
  assert.ok(redirect({}, "http://127.0.0.1:7070", refusal(fp, "http://gpu.internal:7070")));
  assert.ok(redirect({ token: "t", insecureRedirectHttp: true }, "http://cpu:7070", refusal(fp, "http://gpu:7070")));
});

test("loopback means the host the connection uses", () => {
  assert.ok(isLoopback("http://127.0.0.1:1"));
  assert.ok(isLoopback("http://localhost:1"));
  assert.ok(isLoopback("http://[::1]:1"));
  assert.ok(!isLoopback("http://127.0.0.1:1@evil:80"));
  assert.ok(!isLoopback("http://127.0.0.1.evil:80"));
  assert.ok(!isLoopback("http://10.0.0.1:80"));
});

test("workers follow their pool to its shard", { skip: bin === undefined && "cronoq-server not built" }, async () => {
  const pools = '\n[[pools]]\nname = "default"\nmax = 4\n[[pools]]\nname = "llm"\nmax = 4\n[[pools]]\nname = "spare"\nmax = 1\n';
  // Each shard's configuration names the other's port, so the ports are chosen up front. Another
  // process can take one before a server binds it; then both start again on new ports.
  let cpu: Server, gpu: Server, layout: (cpuPools: string, gpuPools: string) => string;
  for (let attempt = 1; ; attempt++) {
    const [cpuGrpc, cpuHttp, gpuGrpc, gpuHttp] = [await freePort(), await freePort(), await freePort(), await freePort()];
    layout = (cpuPools, gpuPools) =>
      `${pools}\n[[shards]]\nname = "cpu"\nnumber = 1\npools = ${cpuPools}\nendpoint = "http://127.0.0.1:${cpuGrpc}"\n` +
      `[[shards]]\nname = "gpu"\nnumber = 2\npools = ${gpuPools}\nendpoint = "http://127.0.0.1:${gpuGrpc}"\n`;
    const shared = layout('["default", "spare"]', '["llm"]');
    cpu = new Server(bin!, cpuGrpc, cpuHttp, 'shard = "cpu"', shared);
    gpu = new Server(bin!, gpuGrpc, gpuHttp, 'shard = "gpu"', shared);
    try {
      await cpu.start();
      await gpu.start();
      break;
    } catch (e) {
      await cpu.stop();
      await gpu.stop();
      if (attempt === 3 || !String(e).includes("Address already in use")) throw e;
    }
  }
  try {
    // Pointed at cpu, a worker for pool llm registers with gpu and runs its tasks.
    const ac = new AbortController();
    let endpoint = "";
    const done = runWorker(
      { target: cpu.target, pool: "llm", token: WORKER, logger: quiet, signal: ac.signal, onRegistered: (r) => (endpoint = r.endpoint) },
      (l) => l.text().toUpperCase(),
    );
    const sub = await connect(cpu.target, { token: CLIENT }).submitAndWait({ pool: "llm", payload: "gpu" }, 20);
    assert.equal(text(sub.result?.output), "GPU");
    assert.equal(endpoint, gpu.target);
    ac.abort();
    await done;
    await cpu.stop();
    await gpu.stop();

    // Shard maps that each name the other server send the worker back and forth until it stops.
    gpu.tables = layout('["default", "llm"]', '["spare"]');
    await cpu.start();
    await gpu.start();
    let followed = 0;
    const counting = { ...quiet, info: (m: string) => void (m.includes("served by another shard") && followed++) };
    await assert.rejects(
      runWorker({ target: cpu.target, pool: "llm", token: WORKER, logger: counting }, (l) => l.payload),
      /redirected 4 times in a row/,
    );
    assert.equal(followed, 3);
  } finally {
    await cpu.stop();
    await gpu.stop();
  }
});
