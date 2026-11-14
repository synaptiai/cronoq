import { create } from "@bufbuild/protobuf";
import { createClient, type Client as RpcClient, type Interceptor, type Transport } from "@connectrpc/connect";

import { CronoqError, wrap } from "./errors.js";
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
  /**
   * What to do when `key` is in use. `"return_existing"` (the default) returns that task and
   * drops this one. `"replace_queued"` lets the newest submission win: a task that has not
   * started is replaced in place (same id and place in line); one that has started keeps its
   * payload, and this task runs after it as a new task with the same key. See
   * {@link Submitted.outcome}. `submitBatch` refuses it; {@link spec} does not carry it.
   * `"reject"` returns the existing task only for a submission that asks for the same thing
   * (payload, content type, pool, kind, priority and dependencies); one that asks for
   * something else fails with a {@link CronoqError} whose `keyConflict` is true, instead of
   * being dropped. In a batch, set it on every task or on none. Left out, the server's
   * default applies.
   */
  onKeyConflict?: "return_existing" | "replace_queued" | "reject";
  /** With `"replace_queued"`: take this submission's place in line. */
  renewPosition?: boolean;
  /**
   * Replaces the pool's limit on how often an attempt of this task may be released by a
   * handler ({@link Release}) without counting as a failure.
   */
  maxReleases?: number;
  /**
   * Tasks of one pool that share an ordering key run one at a time, in the order they were
   * submitted, however many workers are free: use it for work that must not overlap or
   * overtake for one subject (an account, a document, a device). Tasks with different keys do
   * not wait for each other. The order inside a key is submission order, not priority; the
   * task at the front runs at the highest class among the key's waiting tasks. It is not a
   * deduplication key (`key`): two submissions with the same ordering key are two tasks. Not
   * allowed together with `startIn`.
   */
  orderingKey?: string;
  /**
   * With `orderingKey`: if this task ends as failed, halt the key. The tasks behind it stay
   * queued, in place, until {@link Client.resume} or until they are cancelled.
   */
  haltOnFailure?: boolean;
}

/** An ordering key as the server holds it. */
export interface OrderingInfo {
  pool: string;
  key: string;
  /** Tasks of the key that have not finished. */
  tasks: bigint;
  /** The oldest of them: the only one that may run. */
  head?: bigint;
  /** The class the head runs at: the highest among the key's tasks and its floor. */
  effective?: Priority;
  /** Class floor, if any. */
  floor?: Priority;
  /** The floor stays when the key has no tasks left. */
  persist: boolean;
  /** A failed task halted the key: nothing of it runs until it is resumed. */
  halted: boolean;
  /** The reason the key is paused, if it is. */
  paused?: string;
}

/**
 * What {@link Client.pause} and {@link Client.resume} act on: a pool (the default pool if
 * `pool` is left out); with `class`, the tasks of the pool that run at that class; with
 * `orderingKey`, that ordering key of the pool. `class` and `orderingKey` exclude each other.
 */
export interface PauseTarget {
  pool?: string;
  class?: Priority;
  orderingKey?: string;
}

/** The options of a single submission of `t`. */
function submitOptions(t: Task): { onKeyConflict: pb.KeyConflict; renewPosition: boolean } {
  let onKeyConflict: pb.KeyConflict;
  switch (t.onKeyConflict) {
    case undefined:
      onKeyConflict = pb.KeyConflict.UNSPECIFIED;
      break;
    case "return_existing":
      onKeyConflict = pb.KeyConflict.RETURN_EXISTING;
      break;
    case "reject":
      onKeyConflict = pb.KeyConflict.REJECT;
      break;
    case "replace_queued":
      onKeyConflict = pb.KeyConflict.REPLACE_QUEUED;
      break;
    default:
      throw new CronoqError("invalid_argument", `unknown onKeyConflict '${String(t.onKeyConflict)}'`);
  }
  return { onKeyConflict, renewPosition: t.renewPosition ?? false };
}

/** The result of a single submission of `t`. */
function accepted(t: Task, r: pb.SubmitResponse): Submitted {
  if (t.orderingKey && r.outcome === pb.SubmitOutcome.UNSPECIFIED) {
    // A server older than ordering keys ignored the key and admitted the task.
    throw new CronoqError("unimplemented", "the server does not support orderingKey; the task was submitted without it");
  }
  if (t.onKeyConflict === "replace_queued" && r.outcome === pb.SubmitOutcome.UNSPECIFIED) {
    // A server older than the option ignored it: with the key in use, this submission was
    // dropped, not stored.
    throw new CronoqError("unimplemented", "the server does not support onKeyConflict");
  }
  return submitted(r);
}

/** What an accepted submission did. */
export type Outcome = "created" | "existing" | "replaced" | "pending";

/** What an {@link Event} reports. */
export type EventKind = "transition" | "updated" | "replaced" | "successor";

function eventKind(k: pb.EventKind): EventKind {
  switch (k) {
    case pb.EventKind.UPDATED:
      return "updated";
    case pb.EventKind.REPLACED:
      return "replaced";
    case pb.EventKind.SUCCESSOR:
      return "successor";
    default:
      return "transition";
  }
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
    orderingKey: t.orderingKey ?? "",
    maxReleases: t.maxReleases,
    onFailure: t.haltOnFailure ? pb.OrderFailure.HALT : pb.OrderFailure.UNSPECIFIED,
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
  /**
   * What the submission did: `"created"`; `"existing"` (the key was in use and the task is
   * returned unchanged); `"replaced"` (the task had not started and now carries this
   * submission); or `"pending"` (the task has started, so this submission runs after it as a
   * new task with the same key: `id` and `result` are the running task's).
   */
  outcome: Outcome;
}

function outcome(r: pb.SubmitResponse): Outcome {
  switch (r.outcome) {
    case pb.SubmitOutcome.CREATED:
      return "created";
    case pb.SubmitOutcome.EXISTING:
      return "existing";
    case pb.SubmitOutcome.REPLACED:
      return "replaced";
    case pb.SubmitOutcome.PENDING:
      return "pending";
    default:
      // A server older than the field only says whether the key existed.
      return r.duplicate ? "existing" : "created";
  }
}

function submitted(r: pb.SubmitResponse): Submitted {
  return {
    id: r.id,
    duplicate: r.duplicate,
    result: r.result ? result(r.result) : undefined,
    outcome: outcome(r),
  };
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
  /** The task's class at the time of the event (`undefined` from older servers). */
  priority?: Priority;
  /**
   * The event is not a status change: the task was changed by {@link Client.update} and
   * `status` is its unchanged status. Only delivered with `allTransitions`.
   */
  updated: boolean;
  /**
   * What the event reports. Kinds other than `"transition"` are only delivered with
   * `allTransitions`, and `status` is then the task's status at that moment. `"replaced"`: a
   * submission with the task's key replaced it or was stored to run after it. `"successor"`:
   * the task was created from such a stored submission; `message` names the task it follows.
   */
  kind: EventKind;
}

/** What to change on a task that has not finished; fields left out keep their value. */
export interface Update {
  /** Move the task to this class. */
  priority?: Priority;
  /** Set the deadline this many seconds from now. */
  deadlineIn?: number;
  /** Set the deadline to this time. */
  deadlineAt?: Date;
  /** Remove the deadline. */
  clearDeadline?: boolean;
  /** Replace the task's tags (an empty object removes them). */
  tags?: Record<string, string>;
}

/** Result of {@link Client.update}. */
export interface Updated {
  id: bigint;
  /** The task's status, which an update never changes. */
  status: Status;
  /** An attempt is running: it is not interrupted, and the new values apply to what follows. */
  deferred: boolean;
  /** `false` when every value given was already in place. */
  changed: boolean;
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
    return this.call(async () =>
      accepted(task, await this.tasks.submit({ task: spec(task), ...submitOptions(task) }, rpc(opts))),
    );
  }

  /**
   * Submit one task and wait up to `waitTimeout` seconds (default: the server's limit) for it
   * to finish. A {@link CronoqError} with `timeout` set means it is still running.
   */
  submitAndWait(task: Task, waitTimeout?: number, opts: CallOptions = {}): Promise<Submitted> {
    const o = { ...opts, timeout: opts.timeout ?? (waitTimeout === undefined ? undefined : waitTimeout + 5) };
    return this.call(async () =>
      accepted(
        task,
        await this.tasks.submit(
          { task: spec(task), wait: true, waitTimeoutS: waitTimeout, ...submitOptions(task) },
          rpc(o),
        ),
      ),
    );
  }

  /** Submit raw specs atomically; they may depend on each other's keys. */
  submitBatch(tasks: Array<Task | pb.TaskSpec>, opts: CallOptions = {}): Promise<Submitted[]> {
    return this.call(async () => {
      // The mode is one per batch: every task names the same one (raw specs name none).
      const modes = tasks.map((t) => ("$typeName" in t ? undefined : (t as Task).onKeyConflict));
      const specs = tasks.map((t, i) => {
        if ("$typeName" in t) return t as pb.TaskSpec;
        const task = t as Task;
        if (task.onKeyConflict === "replace_queued" || task.renewPosition) {
          throw new CronoqError(
            "invalid_argument",
            `tasks[${i}]: replace_queued and renewPosition are not supported in a batch`,
          );
        }
        if (modes[i] !== modes[0]) {
          throw new CronoqError("invalid_argument", `tasks[${i}]: use the same onKeyConflict for every task of a batch`);
        }
        return spec(task);
      });
      const onKeyConflict = tasks.length > 0 && !("$typeName" in tasks[0]!) ? submitOptions(tasks[0] as Task).onKeyConflict : pb.KeyConflict.UNSPECIFIED;
      // A raw spec names no mode, so it cannot share a batch with tasks that name one.
      if (modes.some((m) => m !== undefined) && tasks.some((t) => "$typeName" in t)) {
        throw new CronoqError("invalid_argument", "a batch with onKeyConflict takes Tasks, not raw specs");
      }
      const r = await this.tasks.submitBatch({ tasks: specs, onKeyConflict }, rpc(opts));
      if (specs.some((t) => t.orderingKey) && r.tasks.some((t) => t.outcome === pb.SubmitOutcome.UNSPECIFIED)) {
        // A server older than ordering keys ignored them and admitted the batch.
        throw new CronoqError("unimplemented", "the server does not support orderingKey; the batch was submitted without it");
      }
      if (onKeyConflict === pb.KeyConflict.REJECT && r.tasks.some((t) => t.outcome === pb.SubmitOutcome.UNSPECIFIED)) {
        // A server older than the option ignored it and admitted the batch.
        throw new CronoqError(
          "unimplemented",
          "the server does not support onKeyConflict for batches; the batch was submitted without it",
        );
      }
      return r.tasks.map(submitted);
    });
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

  /**
   * Change the priority, deadline or tags of a task that has not finished. A waiting task takes
   * the new values at once and keeps its place by submission order in its new class. Rejects
   * with `not_found` for an unknown or finished task and `failed_precondition` for a task with
   * a fixed start.
   */
  update(task: TaskRef, changes: Update, opts: CallOptions = {}): Promise<Updated> {
    return this.call(async () => {
      const r = await this.tasks.update(
        {
          task: ref(task),
          priority: priorityValue(changes.priority),
          deadlineInS: changes.deadlineIn,
          deadlineUnixNs:
            changes.deadlineAt === undefined ? undefined : BigInt(changes.deadlineAt.getTime()) * 1_000_000n,
          clearDeadline: changes.clearDeadline ?? false,
          setTags: changes.tags !== undefined,
          tags: changes.tags ?? {},
        },
        rpc(opts),
      );
      return { id: r.id, status: statusName(r.status), deferred: r.deferred, changed: r.changed };
    });
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
          priority: priorityName(e.priority),
          updated: e.kind === pb.EventKind.UPDATED,
          kind: eventKind(e.kind),
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

  /**
   * An ordering key of `pool` (the default pool if left out): its tasks, the one at the
   * front, the class that one runs at and whether the key is halted (metrics role). A key
   * with no tasks and no kept floor has `tasks` 0.
   */
  ordering(key: string, pool?: string, opts: CallOptions = {}): Promise<OrderingInfo> {
    return this.call(async () => {
      const r = await this.admin.getOrdering({ pool: pool ?? "", key }, rpc(opts));
      return {
        pool: r.pool,
        key: r.key,
        tasks: r.tasks,
        head: r.head,
        effective: priorityName(r.effective),
        floor: r.floor === undefined ? undefined : priorityName(r.floor),
        persist: r.persist,
        halted: r.halted,
        paused: r.paused,
      };
    });
  }

  /** The halted ordering keys of `pool`, in name order (metrics role). */
  haltedOrderings(filter: { pool?: string; limit?: number } = {}, opts: CallOptions = {}): Promise<string[]> {
    return this.call(
      async () =>
        (await this.admin.listHaltedOrderings({ pool: filter.pool ?? "", limit: filter.limit ?? 0 }, rpc(opts))).keys,
    );
  }

  /**
   * Set the class the front of an ordering key never runs below, whatever the classes of the
   * key's tasks; `floor: undefined` removes it. No task's own class changes. The floor goes
   * when the key has no tasks left unless `persist` is set; the server limits how many keys
   * keep one (`resource_exhausted` beyond it). Admin role.
   */
  setOrderingFloor(
    key: string,
    floor: Priority | undefined,
    o: { pool?: string; persist?: boolean } = {},
    opts: CallOptions = {},
  ): Promise<void> {
    return this.call(async () => {
      await this.admin.setOrderingFloor(
        {
          pool: o.pool ?? "",
          key,
          floor: floor === undefined ? undefined : priorityValue(floor),
          persist: o.persist ?? false,
        },
        rpc(opts),
      );
    });
  }

  /**
   * Pause a pool, one of its classes or one of its ordering keys. What is paused stays
   * queued: tasks keep their place and their age and count toward the bounds, and submissions
   * are still accepted. Nothing of it is leased until {@link Client.resume}. Running attempts
   * are not interrupted; a task that retries waits; tasks with a start time are not held.
   * `reason` is shown in snapshots and to workers. Resolves to whether this paused something
   * that was not paused; pausing again replaces the reason. Admin role; credentials pinned to
   * a tenant pause their own ordering keys only.
   */
  pause(target: PauseTarget, reason: string, opts: CallOptions = {}): Promise<boolean> {
    return this.call(
      async () =>
        (
          await this.admin.pause(
            {
              pool: target.pool ?? "",
              priorityClass: target.class === undefined ? undefined : priorityValue(target.class),
              orderingKey: pauseKey(target),
              reason,
            },
            rpc(opts),
          )
        ).paused,
    );
  }

  /**
   * Resume what {@link Client.pause} paused; for an ordering key, also a key that a failed
   * task halted (`haltOnFailure`). Resolves to whether there was anything to resume; resuming
   * what is not paused is not an error. Admin role.
   */
  resume(target: PauseTarget = {}, opts: CallOptions = {}): Promise<boolean> {
    return this.call(
      async () =>
        (
          await this.admin.resume(
            {
              pool: target.pool ?? "",
              priorityClass: target.class === undefined ? undefined : priorityValue(target.class),
              orderingKey: pauseKey(target),
            },
            rpc(opts),
          )
        ).resumed,
    );
  }

  /** The paused ordering keys of `pool` with their reasons, in name order (metrics role). */
  pausedOrderings(
    filter: { pool?: string; limit?: number } = {},
    opts: CallOptions = {},
  ): Promise<Array<{ key: string; reason: string }>> {
    return this.call(async () =>
      (
        await this.admin.listPausedOrderings({ pool: filter.pool ?? "", limit: filter.limit ?? 0 }, rpc(opts))
      ).keys.map((k) => ({ key: k.key, reason: k.reason })),
    );
  }
}

/** The ordering key a pause or resume names, checked. */
function pauseKey(target: PauseTarget): string | undefined {
  if (target.orderingKey === undefined) return undefined;
  if (target.class !== undefined) throw new CronoqError("invalid_argument", "name a class or an orderingKey, not both");
  // Not a way to name the pool: an empty key is a mistake in the caller.
  if (target.orderingKey === "") {
    throw new CronoqError("invalid_argument", "orderingKey must not be empty; leave it out to name the pool");
  }
  return target.orderingKey;
}

function rpc(opts: CallOptions) {
  return { signal: opts.signal, timeoutMs: opts.timeout === undefined ? undefined : opts.timeout * 1000 };
}
