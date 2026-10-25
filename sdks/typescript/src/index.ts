/**
 * TypeScript client for a cronoq server. Import from `@synaptiai/cronoq/node` in Node (gRPC over HTTP/2,
 * clients and workers) or from `@synaptiai/cronoq/web` in browsers (gRPC-Web, clients only).
 *
 * @module
 */
export {
  Client,
  bearer,
  encode,
  isDone,
  json,
  spec,
  text,
  type CallOptions,
  type Cond,
  type Event,
  type Payload,
  type Priority,
  type Result,
  type Status,
  type Submitted,
  type Task,
  type TaskInfo,
  type TaskPage,
  type TaskRef,
} from "./client.js";
export { CancelTask, CronoqError, Permanent, Reject, Retryable } from "./errors.js";
export { MAX_REDIRECTS, PROTOCOL_VERSION, type Handler, type Lease, type WorkerOptions } from "./worker.js";
/** The generated protobuf messages and service descriptors. */
export * as pb from "./gen/cronoq/v1/cronoq_pb.js";
