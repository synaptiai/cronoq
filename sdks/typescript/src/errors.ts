import { Code, ConnectError } from "@connectrpc/connect";

/** A failed call. `code` is the lower-case gRPC status name, e.g. `not_found`. */
export class CronoqError extends Error {
  override name = "CronoqError";
  constructor(
    readonly code: string,
    message: string,
    /** Response metadata (headers and trailers) of the failed call. */
    readonly metadata: Headers = new Headers(),
  ) {
    super(`${code}: ${message}`);
  }

  /** The message without the code. */
  get detail(): string {
    return this.message.slice(this.code.length + 2);
  }

  /** Retrying the same call later can succeed (not the leader, shutting down, queue full). */
  get retryable(): boolean {
    return this.code === "unavailable" || this.code === "resource_exhausted";
  }

  /** A wait ended while the task was still running. */
  get timeout(): boolean {
    return this.code === "deadline_exceeded";
  }

  /** The task, pool or dependency does not exist (or the caller may not see it). */
  get notFound(): boolean {
    return this.code === "not_found";
  }
}

/** The gRPC status name of a Connect code, as the other cronoq clients spell it. */
function codeName(code: Code): string {
  // Connect spells code 1 `Canceled`; gRPC's name is CANCELLED.
  if (code === Code.Canceled) return "cancelled";
  return Code[code].replace(/[A-Z]/g, (c, i: number) => (i ? "_" : "") + c.toLowerCase());
}

/** Wrap an error from a call into a {@link CronoqError}. */
export function wrap(e: unknown): unknown {
  if (e instanceof CronoqError) return e;
  if (e instanceof ConnectError) return new CronoqError(codeName(e.code), e.rawMessage, e.metadata);
  return e;
}

/** Thrown by a worker handler: the task fails without retrying. */
export class Permanent extends Error {
  override name = "Permanent";
}

/** Thrown by a worker handler: retryable failure, retried no earlier than `retryAfter` seconds. */
export class Retryable extends Error {
  override name = "Retryable";
  constructor(
    message: string,
    readonly retryAfter?: number,
  ) {
    super(message);
  }
}

/** Thrown by a worker handler: hand the lease back unrun; the server offers it to another worker. */
export class Reject extends Error {
  override name = "Reject";
}

/** Thrown by a worker handler: end the task as cancelled. */
export class CancelTask extends Error {
  override name = "CancelTask";
}
