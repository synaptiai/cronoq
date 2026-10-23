import { create } from "@bufbuild/protobuf";
import { createClient, type Client as RpcClient, type Interceptor, type Transport } from "@connectrpc/connect";

import { wrap } from "./errors.js";
import * as pb from "./gen/cronoq/v1/cronoq_pb.js";

/**
 * A payload or output: bytes as they are, a string as UTF-8 text, `null`/`undefined` as empty,
 * anything else as JSON.
 */
export type Payload = Uint8Array | string | null | undefined | unknown;

const utf8 = new TextEncoder();
const fromUtf8 = new TextDecoder();

/** Encode a payload, with the content type that goes with it unless one is given. */
export function encode(payload: Payload, contentType?: string): { data: Uint8Array; contentType: string } {
  if (payload instanceof Uint8Array) return { data: payload, contentType: contentType ?? "application/octet-stream" };
  if (typeof payload === "string") return { data: utf8.encode(payload), contentType: contentType ?? "text/plain; charset=utf-8" };
  if (payload === null || payload === undefined) return { data: new Uint8Array(), contentType: contentType ?? "" };
  return { data: utf8.encode(JSON.stringify(payload)), contentType: contentType ?? "application/json" };
}

/** Decode bytes as UTF-8 text. */
export function text(data: Uint8Array | undefined): string {
  return fromUtf8.decode(data ?? new Uint8Array());
}

/** Decode bytes as JSON (`null` when empty). */
export function json<T = unknown>(data: Uint8Array | undefined): T {
  const s = text(data);
  return (s === "" ? null : JSON.parse(s)) as T;
}

/** A task by id or by key. Ids are `bigint`: they use all 64 bits. */
export type TaskRef = bigint | string;

function ref(r: TaskRef): pb.TaskRef {
  if (typeof r === "bigint") return create(pb.TaskRefSchema, { target: { case: "id", value: r } });
  if (typeof r === "string") return create(pb.TaskRefSchema, { target: { case: "key", value: r } });
  throw new TypeError("a task reference is an id (bigint) or a key (string)");
}

/** Priority classes. */
export type Priority = "critical" | "high" | "normal" | "low";

/** Task statuses. */
export type Status =
  | "blocked"
  | "scheduled"
  | "ready"
  | "running"
  | "retrying"
  | "succeeded"
  | "failed"
  | "dead_lettered"
  | "cancelled"
  | "skipped"
  | "missed"
  | "shed";

const TERMINAL = new Set<Status>(["succeeded", "failed", "dead_lettered", "cancelled", "skipped", "missed", "shed"]);

/** Whether a status is terminal. */
export function isDone(s: Status): boolean {
  return TERMINAL.has(s);
}

// protobuf-es drops the enum name prefix: TASK_STATUS_SUCCEEDED is TaskStatus.SUCCEEDED.
export function statusName(s: pb.TaskStatus): Status {
  return (pb.TaskStatus[s] ?? "unspecified").toLowerCase() as Status;
}

function statusValue(s: Status): pb.TaskStatus {
  const v = (pb.TaskStatus as unknown as Record<string, number>)[s.toUpperCase()];
  if (!v) throw new TypeError(`unknown status '${s}'`);
  return v;
}

export function priorityName(p: pb.Priority): Priority | undefined {
  return p === pb.Priority.UNSPECIFIED ? undefined : (pb.Priority[p].toLowerCase() as Priority);
}

function priorityValue(p: Priority | undefined): pb.Priority {
  if (p === undefined) return pb.Priority.UNSPECIFIED;
  const v = (pb.Priority as unknown as Record<string, number>)[p.toUpperCase()];
  if (!v) throw new TypeError(`unknown priority '${p}'`);
  return v;
}

/** When a dependency is satisfied. */
export type Cond = "on_success" | "on_complete" | "on_failure";

const CONDS: Record<Cond, pb.EdgeCond> = {
  on_success: pb.EdgeCond.ON_SUCCESS,
  on_complete: pb.EdgeCond.ON_COMPLETE,
  on_failure: pb.EdgeCond.ON_FAILURE,
};

/** A task to submit. Times are in seconds; unset fields take the server's defaults. */
export interface Task {
  payload?: Payload;
  contentType?: string;
  kind?: string;
  pool?: string;
  /** Makes the submission idempotent: a task with the same key is returned instead. */
  key?: string;
  tenant?: string;
  priority?: Priority;
  /** Bound of each attempt. */
  timeout?: number;
  /** Deadline, relative to the submission. */
  deadline?: number;
  /** Delay before the task may start. */
  startIn?: number;
  /** Tasks this one waits for: a reference (on success) or `{ ref, cond }`. */
  after?: Array<TaskRef | { ref: TaskRef; cond: Cond }>;
  /** Resources held while the task runs (name to units). */
  resources?: Record<string, number>;
  tags?: Record<string, string>;
  /** Worker labels the task requires. */
  labels?: Record<string, string>;
  /** Replaces the pool's retry policy with this many attempts and the server's default backoff. */
  maxAttempts?: number;
}

/** The raw spec of a task, for fields {@link Task} does not cover. */
export function spec(t: Task): pb.TaskSpec {
  const { data, contentType } = encode(t.payload, t.contentType);
  return create(pb.TaskSpecSchema, {
    payload: data,
    contentType,
    kind: t.kind ?? "",
    pool: t.pool ?? "",
    key: t.key ?? "",
    tenant: t.tenant ?? "",
    priority: priorityValue(t.priority),
    timeoutS: t.timeout,
    deadlineInS: t.deadline,
    startInS: t.startIn,
    deps: (t.after ?? []).map((d) =>
      typeof d === "object"
        ? create(pb.DepSchema, { target: ref(d.ref), cond: CONDS[d.cond] })
        : create(pb.DepSchema, { target: ref(d), cond: pb.EdgeCond.ON_SUCCESS }),
    ),
    resources: t.resources ?? {},
    tags: t.tags ?? {},
    labels: t.labels ?? {},
    retry: t.maxAttempts ? create(pb.RetryPolicySchema, { maxAttempts: t.maxAttempts, jitter: true }) : undefined,
  });
}

/** Outcome of a finished task. */
export interface Result {
  id: bigint;
  /** Always terminal. */
  status: Status;
  output?: Uint8Array;
  /** Last error or status message. */
  message: string;
  /** False for a succeeded task whose output is no longer retained. */
  outputAvailable: boolean;
}

function result(r: pb.TaskResult): Result {
  return { id: r.id, status: statusName(r.status), output: r.output, message: r.message, outputAvailable: r.outputAvailable };
}

/** The answer to a submission. */
export interface Submitted {
  /** Id of the task, or of the existing task with the same key. */
  id: bigint;
  duplicate: boolean;
  /** Set by {@link Client.submitAndWait}. */
  result?: Result;
}

function submitted(r: pb.SubmitResponse): Submitted {
  return { id: r.id, duplicate: r.duplicate, result: r.result ? result(r.result) : undefined };
}

/** A task's details; fields other than `id` and `status` are best effort. */
export interface TaskInfo {
  id: bigint;
  status: Status;
  pool: string;
  key: string;
  kind: string;
  tenant: string;
  priority?: Priority;
  attempt: number;
  /** 0 if unknown. */
  maxAttempts: number;
  created?: Date;
  started?: Date;
  finished?: Date;
  message: string;
  outputAvailable: boolean;
}

const date = (ns?: bigint) => (ns === undefined ? undefined : new Date(Number(ns / 1_000_000n)));

function taskInfo(t: pb.TaskInfo): TaskInfo {
  return {
    id: t.id,
    status: statusName(t.status),
    pool: t.pool,
    key: t.key,
    kind: t.kind,
    tenant: t.tenant,
    priority: priorityName(t.priority),
    attempt: t.attempt,
    maxAttempts: t.maxAttempts,
    created: date(t.createdUnixNs),
    started: date(t.startedUnixNs),
    finished: date(t.finishedUnixNs),
    message: t.message,
    outputAvailable: t.outputAvailable,
  };
}

/** A task's status change. */
export interface Event {
  id: bigint;
  pool: string;
  key: string;
  status: Status;
  attempt: number;
  at: Date;
  message: string;
}

/** One page of tasks in id order. */
export interface TaskPage {
  tasks: TaskInfo[];
  /** Pass as `after` for the next page; `0n` when this page is the last. */
  nextAfter: bigint;
  /** Shards that could not be reached; their tasks are missing from the page. */
  unavailableShards: string[];
}

/** Per-call options. */
export interface CallOptions {
  signal?: AbortSignal;
  /** Client-side bound for the whole call, in seconds. */
  timeout?: number;
}

/** Adds `authorization: Bearer <token>` to every call. */
export function bearer(token: string | undefined): Interceptor[] {
  if (!token) return [];
  return [
    (next) => async (req) => {
      req.header.set("authorization", `Bearer ${token}`);
      return next(req);
    },
  ];
}

/** Submits, inspects, waits for and watches tasks over any Connect transport. */
export class Client {
  /** The generated `cronoq.v1.Tasks` client, for calls this class does not wrap. */
  readonly tasks: RpcClient<typeof pb.Tasks>;
  /** The generated `cronoq.v1.Admin` client. */
  readonly admin: RpcClient<typeof pb.Admin>;

  constructor(transport: Transport) {
    this.tasks = createClient(pb.Tasks, transport);
    this.admin = createClient(pb.Admin, transport);
  }

  private async call<T>(f: () => Promise<T>): Promise<T> {
    try {
      return await f();
    } catch (e) {
      throw wrap(e);
    }
  }

  /** Submit one task. */
  submit(task: Task, opts: CallOptions = {}): Promise<Submitted> {
    return this.call(async () => submitted(await this.tasks.submit({ task: spec(task) }, rpc(opts))));
  }

  /**
   * Submit one task and wait up to `waitTimeout` seconds (default: the server's limit) for it
   * to finish. A {@link CronoqError} with `timeout` set means it is still running.
   */
  submitAndWait(task: Task, waitTimeout?: number, opts: CallOptions = {}): Promise<Submitted> {
    const o = { ...opts, timeout: opts.timeout ?? (waitTimeout === undefined ? undefined : waitTimeout + 5) };
    return this.call(async () =>
      submitted(await this.tasks.submit({ task: spec(task), wait: true, waitTimeoutS: waitTimeout }, rpc(o))),
    );
  }

  /** Submit raw specs atomically; they may depend on each other's keys. */
  submitBatch(tasks: Array<Task | pb.TaskSpec>, opts: CallOptions = {}): Promise<Submitted[]> {
    const specs = tasks.map((t) => ("$typeName" in t ? (t as pb.TaskSpec) : spec(t as Task)));
    return this.call(async () => (await this.tasks.submitBatch({ tasks: specs }, rpc(opts))).tasks.map(submitted));
  }

  /** A task's details. */
  get(task: TaskRef, opts: CallOptions = {}): Promise<TaskInfo> {
    return this.call(async () => taskInfo(await this.tasks.get({ task: ref(task) }, rpc(opts))));
  }

  /**
   * Wait up to `timeout` seconds (default: the server's limit) for a task to finish. A
   * {@link CronoqError} with `timeout` set means it is still running.
   */
  wait(task: TaskRef, timeout?: number, opts: CallOptions = {}): Promise<Result> {
    const o = { ...opts, timeout: opts.timeout ?? (timeout === undefined ? undefined : timeout + 5) };
    return this.call(async () => result(await this.tasks.wait({ task: ref(task), timeoutS: timeout }, rpc(o))));
  }

  /** Cancel a task that has not finished; resolves to whether it did. */
  cancel(task: TaskRef, opts: CallOptions = {}): Promise<boolean> {
    return this.call(async () => (await this.tasks.cancel({ task: ref(task) }, rpc(opts))).cancelled);
  }

  /** One page of tasks by pool and status. */
  listTasks(
    filter: { pools?: string[]; statuses?: Status[]; after?: bigint; limit?: number } = {},
    opts: CallOptions = {},
  ): Promise<TaskPage> {
    return this.call(async () => {
      const r = await this.tasks.listTasks(
        {
          pools: filter.pools ?? [],
          statuses: (filter.statuses ?? []).map(statusValue),
          after: filter.after ?? 0n,
          limit: filter.limit ?? 0,
        },
        rpc(opts),
      );
      return { tasks: r.tasks.map(taskInfo), nextAfter: r.nextAfter, unavailableShards: r.unavailableShards };
    });
  }

  /**
   * Status events until `signal` aborts or the stream fails. The stream fails with
   * `unavailable` when the server shuts down or loses leadership; watch again to continue.
   */
  async *watch(
    filter: { pools?: string[]; ids?: bigint[]; keys?: string[]; allTransitions?: boolean } = {},
    opts: { signal?: AbortSignal } = {},
  ): AsyncGenerator<Event> {
    const stream = this.tasks.watch(
      { pools: filter.pools ?? [], ids: filter.ids ?? [], keys: filter.keys ?? [], allTransitions: filter.allTransitions ?? false },
      { signal: opts.signal },
    );
    try {
      for await (const e of stream) {
        yield {
          id: e.id,
          pool: e.pool,
          key: e.key,
          status: statusName(e.status),
          attempt: e.attempt,
          at: new Date(Number(e.atUnixNs / 1_000_000n)),
          message: e.message,
        };
      }
    } catch (e) {
      if (opts.signal?.aborted) return;
      throw wrap(e);
    }
  }

  /** Pools, queues and totals (metrics role). */
  snapshot(opts: CallOptions = {}): Promise<pb.SnapshotResponse> {
    return this.call(() => this.admin.snapshot({}, rpc(opts)));
  }
}

function rpc(opts: CallOptions) {
  return { signal: opts.signal, timeoutMs: opts.timeout === undefined ? undefined : opts.timeout * 1000 };
}
