// Runs in the browser: calls the server over gRPC-Web from the page's origin.
import { connectWeb, text } from "../../src/web.js";

async function run(target: string, token: string) {
  const c = connectWeb(target, { token });
  const ac = new AbortController();
  const events: string[] = [];
  const watching = (async () => {
    for await (const e of c.watch({ keys: ["from-the-browser"] }, { signal: ac.signal })) {
      events.push(e.status);
      if (e.status === "succeeded") ac.abort();
    }
  })();
  await new Promise((r) => setTimeout(r, 200));
  const sub = await c.submitAndWait({ payload: "browser", key: "from-the-browser" }, 15);
  const info = await c.get("from-the-browser");
  await watching;
  return { status: sub.result?.status, output: text(sub.result?.output), id: String(sub.id), got: String(info.id), events };
}

(globalThis as { runTest?: typeof run }).runTest = run;
