import { create } from "@bufbuild/protobuf";
import { createClient, type Transport } from "@connectrpc/connect";

import { encode, priorityName, text, json, type Payload, type Priority } from "./client.js";
import { CancelTask, CronoqError, Permanent, Reject, Retryable, wrap } from "./errors.js";
import * as pb from "./gen/cronoq/v1/cronoq_pb.js";

/** Worker protocol version this package speaks. */
export const PROTOCOL_VERSION = 3;

/** Redirects to other shards a worker follows in a row before it gives up. */
export const MAX_REDIRECTS = 3;

/** One attempt at a task, given to a {@link Handler}. */
export interface Lease {
  /** Lease id; it carries the leader's epoch, so it never repeats. */
  id: bigint;
  taskId: bigint;
  /**
   * Attempt number. `(taskId, attempt)` is a fencing token: attempt numbers never repeat for a
   * task, also across failovers, so record the pair with the side effect and let only the
   * newest attempt write.
   */
  attempt: number;
  payload: Uint8Array;
  contentType: string;
  kind: string;
  key: string;
  pool: string;
  priority?: Priority;
  tags: Record<string, string>;
  /** Timeout of this attempt in seconds. */
  timeout?: number;
  /** Deadline of the task. */
  deadline?: Date;
  /** Aborts when the server aborts the attempt (timeout, cancellation, lease loss) or the drain timeout passes. */
  signal: AbortSignal;
  /** The payload as UTF-8 text. */
  text(): string;
  /** The payload as JSON. */
  json<T = unknown>(): T;
}

/**
 * Runs one lease and returns its output (see {@link Payload}). Throw {@link Permanent},
 * {@link Retryable} (with `retryAfter`), {@link Reject} or {@link CancelTask} to control the
 * outcome; any other error is a retryable failure. When `lease.signal` aborts, the outcome is
 * not reported.
 */
export type Handler = (lease: Lease) => Promise<Payload> | Payload;

/** Options of {@link runWorker}. Times are in seconds. */
export interface WorkerOptions {
  /** The configured server endpoint (`http://` or `https://`). */
  target: string;
  pool: string;
  /** Leases run at once (default 1; the server may grant fewer). */
  slots?: number;
  /** Bearer token (role worker). */
  token?: string;
  /** Shown in server logs and worker listings. */
  name?: string;
  labels?: Record<string, string>;
  /** Stop when the connection fails instead of reconnecting. */
  noReconnect?: boolean;
  /** Reconnect delay bounds (defaults 0.2 and 10). */
  initialBackoff?: number;
  maxBackoff?: number;
  /** How long running handlers may finish after `signal` aborts (default 30). */
  drainTimeout?: number;
  /**
   * Follow a shard redirect to plain `http://` on another host with the token. Without it,
   * redirects keep TLS and use plain http only to loopback. For trusted networks only.
   */
  insecureRedirectHttp?: boolean;
  /** Stops the worker: it drains running handlers and resolves. */
  signal?: AbortSignal;
  /** Called once per registration with the server's grant. */
  onRegistered?: (info: { workerId: bigint; slots: number; endpoint: string }) => void;
  /** Receives log messages (default: `console`). */
  logger?: Pick<Console, "info" | "warn" | "error">;
}

/** Opens a transport to an endpoint: given the configured one or a shard's. */
export type TransportFactory = (endpoint: string) => Transport;

const ENDPOINT_HEADER = "cronoq-shard-endpoint";

const FATAL = new Set([
  "unauthenticated",
  "permission_denied",
  "not_found",
  "invalid_argument",
  "failed_precondition",
  "unimplemented",
]);

/** Host of an http(s) endpoint, or undefined if it is not a plain `scheme://host:port`. */
function hostOf(endpoint: string): string | undefined {
  let u: URL;
  try {
    u = new URL(endpoint);
  } catch {
    return undefined;
  }
  if ((u.protocol !== "http:" && u.protocol !== "https:") || u.username || u.password) return undefined;
  if ((u.pathname !== "/" && u.pathname !== "") || u.search || u.hash) return undefined;
  return u.hostname;
}

/** Whether an endpoint's host is `localhost` or a loopback address. */
export function isLoopback(endpoint: string): boolean {
  const host = hostOf(endpoint)?.replace(/^\[|\]$/g, "").toLowerCase();
  if (host === undefined) return false;
  if (host === "localhost" || host === "::1") return true;
  return /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host);
}

/**
 * The endpoint a refusal sends the worker to, `undefined` when it is not a redirect. Throws a
 * {@link CronoqError} when the endpoint may not be followed from `current`.
 */
export function redirect(opts: Pick<WorkerOptions, "token" | "insecureRedirectHttp">, current: string, e: unknown): string | undefined {
  if (!(e instanceof CronoqError) || e.code !== "failed_precondition") return undefined;
  const target = e.metadata.get(ENDPOINT_HEADER);
  if (!target) return undefined;
  const refuse = (why: string) =>
    new CronoqError("failed_precondition", `the server redirected the worker to '${target}', ${why}; refusing`);
  if (!target.startsWith("http://") && !target.startsWith("https://")) throw refuse("which is not an http(s) endpoint");
  if (hostOf(target) === undefined) throw refuse("which is not a plain host and port");
  const plain = target.startsWith("http://");
  if (plain && current.startsWith("https://")) throw refuse("which would move from https to plain http");
  if (plain && opts.token && !isLoopback(target)) {
    if (!opts.insecureRedirectHttp) {
      throw refuse("which would send its token over plain http to another host (insecureRedirectHttp allows it)");
    }
  }
  return target;
}

class Queue<T> implements AsyncIterable<T> {
  private items: T[] = [];
  private waiting?: () => void;
  private closed = false;

  push(item: T): void {
    if (this.closed) return;
    this.items.push(item);
    this.waiting?.();
  }

  close(): void {
    this.closed = true;
    this.waiting?.();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    for (;;) {
      while (this.items.length) yield this.items.shift()!;
      if (this.closed) return;
      await new Promise<void>((r) => (this.waiting = r));
      this.waiting = undefined;
    }
  }
}

const sleep = (s: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) return resolve();
    const t = setTimeout(done, s * 1000);
    function done() {
      clearTimeout(t);
      signal?.removeEventListener("abort", done);
      resolve();
    }
    signal?.addEventListener("abort", done, { once: true });
  });

/** Let Node exit while only this timer is pending. */
function unref(t: unknown): void {
  (t as { unref?: () => void }).unref?.();
}

const msg = (m: pb.WorkerMessage["msg"]) => create(pb.WorkerMessageSchema, { msg: m });

type SessionEnd =
  | { kind: "shutdown" }
  | { kind: "ended"; reason: string; now: boolean }
  | { kind: "failed"; error: unknown; connectFailed: boolean };

/**
 * Serve a pool: register with the server, run `handler` for each lease (at most `slots` at a
 * time) with heartbeats, and reconnect with backoff when the connection drops or the server
 * steps down, following the pool to the shard that serves it. Resolves after `signal` aborts
 * and running handlers drained; rejects for failures retrying cannot fix (bad credentials,
 * unknown pool, ...), or for any failure with `noReconnect`.
 */
export async function runWorker(opts: WorkerOptions, handler: Handler, transport: TransportFactory): Promise<void> {
  const log = opts.logger ?? console;
  const o = {
    slots: 1,
    initialBackoff: 0.2,
    maxBackoff: 10,
    drainTimeout: 30,
    ...opts,
  };
  if (hostOf(o.target) === undefined) throw new TypeError(`target '${o.target}' must be http(s)://host:port`);
  let backoff = o.initialBackoff;
  let current = o.target;
  let redirects = 0;
  for (;;) {
    const end = await session(o, current, handler, transport, log, () => (backoff = o.initialBackoff));
    if (end.kind === "shutdown") return;
    if (end.kind === "failed") {
      const to = redirect(o, current, end.error);
      if (to !== undefined) {
        if (++redirects > MAX_REDIRECTS) {
          throw new CronoqError("failed_precondition", `worker redirected more than ${MAX_REDIRECTS} times in a row: ${(end.error as Error).message}`);
        }
        log.info(`cronoq: pool '${o.pool}' is served by another shard; connecting to ${to}`);
        current = to;
        if (o.signal?.aborted) return;
        continue;
      }
    }
    redirects = 0;
    if (end.kind === "failed" && end.connectFailed && current !== o.target) {
      log.warn(`cronoq: shard at ${current} unreachable; returning to ${o.target}`);
      current = o.target;
    }
    if (end.kind === "failed" && end.error instanceof CronoqError && FATAL.has(end.error.code)) throw end.error;
    if (o.noReconnect) {
      throw end.kind === "failed" ? end.error : new CronoqError("unavailable", `worker stream ended: ${end.reason}`);
    }
    const delay = end.kind === "ended" && end.now ? 0 : backoff;
    if (end.kind === "failed") log.warn(`cronoq: worker connection failed (${(end.error as Error).message}); reconnecting in ${delay}s`);
    else log.info(`cronoq: worker stream ended (${end.reason}); reconnecting in ${delay}s`);
    await sleep(delay, o.signal);
    if (o.signal?.aborted) return;
    backoff = Math.min(backoff * 2, o.maxBackoff);
  }
}

async function session(
  o: WorkerOptions & { slots: number; drainTimeout: number },
  endpoint: string,
  handler: Handler,
  transport: TransportFactory,
  log: Pick<Console, "info" | "warn" | "error">,
  registeredOk: () => void,
): Promise<SessionEnd> {
  const client = createClient(pb.Workers, transport(endpoint));
  const outbox = new Queue<pb.WorkerMessage>();
  const stream = new AbortController();
  const headers = o.token ? { authorization: `Bearer ${o.token}` } : undefined;
  outbox.push(
    msg({
      case: "register",
      value: create(pb.RegisterSchema, {
        pool: o.pool,
        slots: o.slots,
        workerName: o.name ?? "",
        protocolVersion: PROTOCOL_VERSION,
        labels: o.labels ?? {},
      }),
    }),
  );
  const running = new Map<bigint, { abort: AbortController; aborted: boolean; done: Promise<void> }>();
  let registered = false;
  let draining = false;
  let slots = o.slots;
  let ttl = 0;
  let maxOutput = 0;
  let idle: (() => void) | undefined;

  const drain = async () => {
    draining = true;
    outbox.push(msg({ case: "setSlots", value: create(pb.SetSlotsSchema, { slots: 0 }) }));
    const deadline = Date.now() + o.drainTimeout * 1000;
    while (running.size && Date.now() < deadline) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      await new Promise<void>((r) => {
        idle = r;
        timer = setTimeout(r, deadline - Date.now());
      });
      clearTimeout(timer);
    }
    // Handlers still running report a retryable failure as they end; one that ignores its
    // signal gets 5 more seconds, then its outcome is dropped (the server retries the task).
    for (const r of running.values()) r.abort.abort();
    let grace: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      Promise.all([...running.values()].map((r) => r.done)),
      new Promise<void>((r) => (grace = setTimeout(r, 5000))),
    ]);
    clearTimeout(grace);
    outbox.close();
    // The server ends the stream once it has every outcome; do not wait forever.
    unref(setTimeout(() => stream.abort(), 5000));
  };
  let drained: Promise<void> | undefined;
  const onStop = () => {
    if (registered && !drained) drained = drain();
  };
  o.signal?.addEventListener("abort", onStop, { once: true });

  try {
    for await (const m of client.work(outbox, { headers, signal: stream.signal })) {
      switch (m.msg.case) {
        case "registered": {
          const r = m.msg.value;
          registered = true;
          registeredOk();
          // Version-1 servers do not report a grant.
          slots = r.slots || o.slots;
          ttl = r.leaseTtlS ?? 0;
          maxOutput = Number(r.maxMessageBytes) - 1024;
          log.info(`cronoq: worker ${r.workerId} registered for pool '${o.pool}' with ${slots} slots`);
          o.onRegistered?.({ workerId: r.workerId, slots, endpoint });
          if (o.signal?.aborted) onStop();
          break;
        }
        case "lease": {
          const l = m.msg.value;
          if (draining || running.size >= slots) {
            const reason = draining ? "worker is draining" : "no free slot";
            outbox.push(msg({ case: "nack", value: create(pb.NackSchema, { lease: l.lease, reason }) }));
            break;
          }
          const entry = { abort: new AbortController(), aborted: false, done: Promise.resolve() };
          running.set(l.lease, entry);
          entry.done = run(l, entry.abort.signal, l.leaseTtlS ?? ttl, maxOutput, handler, outbox, log).then((out) => {
            running.delete(l.lease);
            if (!entry.aborted && out) outbox.push(out);
            idle?.();
          });
          break;
        }
        case "abort": {
          const r = running.get(m.msg.value.lease);
          if (r) {
            r.aborted = true;
            r.abort.abort();
          }
          break;
        }
        case "shutdown": {
          if (draining) break;
          const s = m.msg.value;
          return { kind: "ended", reason: `server shutting down: ${s.reason}`, now: s.notLeader };
        }
      }
    }
    if (drained) {
      await drained;
      return { kind: "shutdown" };
    }
    return { kind: "ended", reason: "server closed the stream", now: false };
  } catch (e) {
    if (drained) {
      await drained;
      return { kind: "shutdown" };
    }
    const error = wrap(e);
    const connectFailed = !registered && error instanceof CronoqError && error.code === "unavailable";
    return { kind: "failed", error, connectFailed };
  } finally {
    o.signal?.removeEventListener("abort", onStop);
    for (const r of running.values()) r.abort.abort();
    outbox.close();
    stream.abort();
  }
}

async function run(
  l: pb.Lease,
  signal: AbortSignal,
  ttl: number,
  maxOutput: number,
  handler: Handler,
  outbox: Queue<pb.WorkerMessage>,
  log: Pick<Console, "error">,
): Promise<pb.WorkerMessage | undefined> {
  const id = l.lease;
  const beat =
    ttl > 0
      ? setInterval(
          () => outbox.push(msg({ case: "heartbeat", value: create(pb.HeartbeatSchema, { lease: id }) })),
          Math.max((ttl * 1000) / 3, 10),
        )
      : undefined;
  // An aborted attempt is over for the server, even while its handler keeps running.
  signal.addEventListener("abort", () => clearInterval(beat), { once: true });
  const lease: Lease = {
    id,
    taskId: l.taskId,
    attempt: l.attempt,
    payload: l.payload,
    contentType: l.contentType,
    kind: l.kind,
    key: l.key,
    pool: l.pool,
    priority: priorityName(l.priority),
    tags: l.tags,
    timeout: l.timeoutS,
    deadline: l.deadlineUnixNs === undefined ? undefined : new Date(Number(l.deadlineUnixNs / 1_000_000n)),
    signal,
    text: () => text(l.payload),
    json: <T>() => json<T>(l.payload),
  };
  const fail = (message: string, permanent: boolean, retryAfter?: number) =>
    msg({ case: "fail", value: create(pb.FailSchema, { lease: id, message, permanent, retryAfterS: retryAfter }) });
  try {
    const out = await handler(lease);
    let data: Uint8Array;
    let contentType: string;
    try {
      ({ data, contentType } = encode(out));
    } catch (e) {
      return fail(`handler output is not JSON-serialisable: ${(e as Error).message}`, true);
    }
    if (maxOutput > 0 && data.length > maxOutput) {
      return fail(`output of ${data.length} bytes exceeds the server's message limit`, true);
    }
    return msg({ case: "complete", value: create(pb.CompleteSchema, { lease: id, output: data, contentType }) });
  } catch (e) {
    if (e instanceof CancelTask) return msg({ case: "cancelled", value: create(pb.CancelledSchema, { lease: id }) });
    if (e instanceof Reject) return msg({ case: "nack", value: create(pb.NackSchema, { lease: id, reason: e.message }) });
    if (e instanceof Permanent) return fail(e.message, true);
    if (e instanceof Retryable) return fail(e.message, false, e.retryAfter);
    if (!(e instanceof Error)) log.error("cronoq: handler threw a non-error", e);
    return fail(e instanceof Error ? e.message : String(e), false);
  } finally {
    if (beat) clearInterval(beat);
  }
}
