// Bundles page.ts, serves it from its own origin, and runs it in headless Chromium against a
// server that allows that origin for gRPC-Web, with a Node worker serving the pool.
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { chromium } from "playwright";

import { runWorker } from "../../src/node.js";
import { CLIENT, DEFAULT_POOL, Server, WORKER, serverBinary } from "../server.js";

const here = dirname(fileURLToPath(import.meta.url));
// Compiled to dist/test/browser; the source page sits in test/browser.
const page = join(here, "../../../test/browser/page.ts");

const bin = serverBinary();
if (!bin) {
  console.log("skipped: cronoq-server not built");
  process.exit(0);
}
const bundle = await build({ entryPoints: [page], bundle: true, format: "esm", platform: "browser", write: false, target: "es2022" });
const js = bundle.outputFiles[0]!.text;
const http = createServer((req, res) => {
  if (req.url === "/page.js") {
    res.writeHead(200, { "content-type": "text/javascript" }).end(js);
  } else {
    res.writeHead(200, { "content-type": "text/html" }).end('<!doctype html><script type="module" src="/page.js"></script>');
  }
});
// The page server keeps the port it is given, so the server's ports cannot collide with it.
await new Promise<void>((r) => http.listen(0, "127.0.0.1", r));
const origin = `http://127.0.0.1:${(http.address() as { port: number }).port}`;

const server = await Server.create(bin, "", `${DEFAULT_POOL}\n[server.grpc_web]\nallowed_origins = ["${origin}"]\n`);
await server.start();
const quiet = { info() {}, warn() {}, error() {} };
const ac = new AbortController();
const worker = runWorker({ target: server.target, pool: "default", token: WORKER, logger: quiet, signal: ac.signal }, (l) =>
  l.text().toUpperCase(),
);

const browser = await chromium.launch();
try {
  const tab = await browser.newPage();
  const errors: string[] = [];
  tab.on("pageerror", (e) => errors.push(e.message));
  await tab.goto(origin);
  await tab.waitForFunction(() => "runTest" in globalThis);
  const result = await tab.evaluate(
    ({ target, token }) => (globalThis as unknown as { runTest: (t: string, k: string) => Promise<unknown> }).runTest(target, token),
    { target: server.target, token: CLIENT },
  );
  assert.deepEqual(errors, []);
  const r = result as { status: string; output: string; id: string; got: string; events: string[] };
  assert.equal(r.status, "succeeded");
  assert.equal(r.output, "BROWSER");
  assert.equal(r.got, r.id);
  assert.ok(r.events.includes("succeeded"), `events: ${r.events}`);

  // Another origin is refused by the browser's CORS check.
  const other = await browser.newPage();
  await other.goto("about:blank");
  const refused = await other.evaluate(async (target) => {
    try {
      await fetch(`${target}/cronoq.v1.Tasks/Get`, {
        method: "POST",
        headers: { "content-type": "application/grpc-web+proto", "x-grpc-web": "1" },
        body: new Uint8Array(5),
      });
      return false;
    } catch {
      return true;
    }
  }, server.target);
  assert.ok(refused, "a page from another origin could call the server");
  console.log("browser test passed:", JSON.stringify(r));
} finally {
  await browser.close();
  http.close();
  ac.abort();
  await worker;
  await server.stop();
}
