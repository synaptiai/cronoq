/**
 * Node entry point: gRPC over HTTP/2, for clients and workers.
 *
 * ```ts
 * import { connect, runWorker } from "@synaptiai/cronoq/node";
 * const c = connect("http://127.0.0.1:7070", { token: process.env.CRONOQ_TOKEN });
 * const sub = await c.submitAndWait({ payload: { to: "ada" } }, 30);
 * ```
 *
 * @module
 */
import type { Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";

import { Client, bearer } from "./client.js";
import { isLoopback, runWorker as run, type Handler, type WorkerOptions } from "./worker.js";

export * from "./index.js";

/** TLS settings for `https://` endpoints (PEM strings or buffers). */
export interface Tls {
  /** CA certificates to trust (default: Node's roots). */
  ca?: string | Buffer;
  /** Client certificate and key, for mTLS. */
  cert?: string | Buffer;
  key?: string | Buffer;
  /** Server name to verify (default: the endpoint host). Applies to the configured endpoint only. */
  servername?: string;
}

/** Connection options. */
export interface ConnectOptions {
  /** Bearer token sent with every call. */
  token?: string;
  tls?: Tls;
  /** Largest message sent or received (default 64 MiB). */
  maxMessageBytes?: number;
  /** Receives warnings (default: `console`). */
  logger?: Pick<Console, "warn">;
}

/** A gRPC transport to `endpoint` (`http://host:port` or `https://host:port`). */
export function transport(endpoint: string, opts: ConnectOptions = {}): Transport {
  const https = endpoint.startsWith("https://");
  if (!https && !endpoint.startsWith("http://")) throw new TypeError(`endpoint '${endpoint}' must start with http:// or https://`);
  if (!https && opts.tls) throw new TypeError(`TLS settings need an https:// endpoint, not '${endpoint}'`);
  if (!https && opts.token && !isLoopback(endpoint)) {
    (opts.logger ?? console).warn(`cronoq: the token is sent over plain http to ${endpoint}; use https outside a trusted network`);
  }
  const max = opts.maxMessageBytes ?? 64 << 20;
  return createGrpcTransport({
    baseUrl: endpoint,
    interceptors: bearer(opts.token),
    readMaxBytes: max,
    writeMaxBytes: max,
    nodeOptions: https ? { ...opts.tls } : undefined,
  });
}

/** A client for the server at `endpoint`. The connection opens on the first call. */
export function connect(endpoint: string, opts: ConnectOptions = {}): Client {
  return new Client(transport(endpoint, opts));
}

/**
 * Serve a pool until `opts.signal` aborts (see the core `runWorker` docs in the reference):
 * slots, heartbeats, reconnects, draining, and redirects to the pool's shard. TLS settings
 * carry over to a shard's endpoint, except `servername`.
 */
export function runWorker(opts: WorkerOptions & { tls?: Tls; maxMessageBytes?: number }, handler: Handler): Promise<void> {
  if (opts.tls && !opts.target.startsWith("https://")) {
    return Promise.reject(new TypeError(`worker TLS settings need an https:// target, not '${opts.target}'`));
  }
  if (opts.token && opts.target.startsWith("http://") && !isLoopback(opts.target)) {
    (opts.logger ?? console).warn(`cronoq: the worker token is sent over plain http to ${opts.target}; use https outside a trusted network`);
  }
  return run(opts, handler, (endpoint) => {
    const tls = opts.tls && endpoint !== opts.target ? { ...opts.tls, servername: undefined } : opts.tls;
    // The worker sends its token as call metadata itself.
    return transport(endpoint, { tls: endpoint.startsWith("https://") ? tls : undefined, maxMessageBytes: opts.maxMessageBytes, logger: opts.logger });
  });
}
