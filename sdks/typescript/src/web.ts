/**
 * Browser entry point: gRPC-Web, for clients (unary calls and `watch`). The server must allow
 * the page's origin in `[server.grpc_web]`. Workers need `@synaptiai/cronoq/node`.
 *
 * ```ts
 * import { connectWeb } from "@synaptiai/cronoq/web";
 * const c = connectWeb("https://cronoq.example.com:7070", { token });
 * for await (const e of c.watch({ pools: ["default"] })) console.log(e.id, e.status);
 * ```
 *
 * @module
 */
import type { Transport } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";

import { Client, bearer } from "./client.js";

export * from "./index.js";

/** A gRPC-Web transport to `baseUrl`, the server's gRPC listener. */
export function transport(baseUrl: string, opts: { token?: string } = {}): Transport {
  return createGrpcWebTransport({ baseUrl, interceptors: bearer(opts.token) });
}

/** A client for the server's gRPC listener at `baseUrl`, over gRPC-Web. */
export function connectWeb(baseUrl: string, opts: { token?: string } = {}): Client {
  return new Client(transport(baseUrl, opts));
}
