// The cronoq scheduler embedded in a Swift program, without a server.
//
// EXPERIMENTAL until cronoq 1.0.
//
// The scheduler lives in the cronoq C library. The program submits tasks (opaque bytes) and
// runs them itself: threads take one attempt at a time with `Scheduler.next`, do the work,
// and report the result on the `Lease`.

import CCronoq
import Foundation

/// The revision of the C API this package was written for.
public let abiVersion: UInt32 = 1

/// An error from the scheduler.
public enum CronoqError: Error, Equatable {
    /// `next` found no attempt in time.
    case timeout
    /// `next` found no attempt in time and the pool is paused, with this reason.
    case paused(reason: String)
    /// The scheduler was shut down.
    case shutdown
    /// An argument or an option is wrong.
    case invalidArgument(String)
    /// There is no such live task.
    case notFound(String)
    /// The lease is over (the attempt timed out, the task was cancelled, or the lease was
    /// already ended). Stop working on it.
    case leaseGone
    /// The pool is full, or a limit of the scheduler is reached.
    case full(String)
    /// A live task with this key exists and the submission conflicts with it.
    case conflict(String)
    /// The store failed.
    case store(String)
    /// A fault inside the library.
    case `internal`(String)
}

/// The class of a task.
public enum Priority: String, Codable, Sendable {
    case critical, high, normal, low

    var index: Int32 {
        switch self {
        case .critical: return 0
        case .high: return 1
        case .normal: return 2
        case .low: return 3
        }
    }
}

/// A retry policy. Fields left nil keep their default.
public struct Retry: Codable, Sendable {
    enum CodingKeys: String, CodingKey {
        case maxAttempts = "max_attempts"
        case initialBackoffMs = "initial_backoff_ms"
        case multiplier
        case maxBackoffMs = "max_backoff_ms"
        case jitter
    }

    /// The number of attempts in all (default 3).
    public var maxAttempts: UInt32?
    /// The wait before the second attempt (default 1000).
    public var initialBackoffMs: UInt64?
    /// Multiplies the wait after each attempt (default 2).
    public var multiplier: Double?
    /// The longest wait (default 60000).
    public var maxBackoffMs: UInt64?
    /// Randomises the wait (default true).
    public var jitter: Bool?

    public init(
        maxAttempts: UInt32? = nil, initialBackoffMs: UInt64? = nil, multiplier: Double? = nil,
        maxBackoffMs: UInt64? = nil, jitter: Bool? = nil
    ) {
        self.maxAttempts = maxAttempts
        self.initialBackoffMs = initialBackoffMs
        self.multiplier = multiplier
        self.maxBackoffMs = maxBackoffMs
        self.jitter = jitter
    }
}

/// One value per class; nil keeps the default of that class.
public struct PerClass: Codable, Sendable {
    public var critical: Double?
    public var high: Double?
    public var normal: Double?
    public var low: Double?

    public init(critical: Double? = nil, high: Double? = nil, normal: Double? = nil, low: Double? = nil) {
        self.critical = critical
        self.high = high
        self.normal = normal
        self.low = low
    }
}

/// How classes are weighed and how waiting tasks age.
public struct PrioritySettings: Codable, Sendable {
    enum CodingKeys: String, CodingKey {
        case base
        case agingRate = "aging_rate"
        case ageCap = "age_cap"
        case allowOvertakingHigh = "allow_overtaking_high"
    }

    public var base: PerClass?
    public var agingRate: PerClass?
    public var ageCap: PerClass?
    public var allowOvertakingHigh: Bool?

    public init(
        base: PerClass? = nil, agingRate: PerClass? = nil, ageCap: PerClass? = nil,
        allowOvertakingHigh: Bool? = nil
    ) {
        self.base = base
        self.agingRate = agingRate
        self.ageCap = ageCap
        self.allowOvertakingHigh = allowOvertakingHigh
    }
}

/// How a deadline raises a task.
public struct UrgencySettings: Codable, Sendable {
    enum CodingKeys: String, CodingKey {
        case max
        case windowS = "window_s"
        case overdueRate = "overdue_rate"
        case overdueCap = "overdue_cap"
    }

    public var max: Double?
    public var windowS: Double?
    public var overdueRate: Double?
    public var overdueCap: Double?

    public init(max: Double? = nil, windowS: Double? = nil, overdueRate: Double? = nil, overdueCap: Double? = nil) {
        self.max = max
        self.windowS = windowS
        self.overdueRate = overdueRate
        self.overdueCap = overdueCap
    }
}

/// One pool.
public struct Pool: Codable, Sendable {
    enum CodingKeys: String, CodingKey {
        case name
        case maxLeases = "max_leases"
        case maxPending = "max_pending"
        case overflow
        case retry
        case maxReleases = "max_releases"
        case leaseTtlMs = "lease_ttl_ms"
        case priority
        case urgency
    }

    public var name: String
    /// The most attempts the program may hold at a time (default 256).
    public var maxLeases: UInt32?
    /// Bounds the queue (default: no bound).
    public var maxPending: UInt64?
    /// What a full queue does: "reject" (default), "shed_lowest" or "shed_oldest".
    public var overflow: String?
    public var retry: Retry?
    /// How often an attempt of one task may be released without counting as a failure
    /// (default 10).
    public var maxReleases: UInt32?
    /// Makes leases expire unless `heartbeat` is called within this time.
    public var leaseTtlMs: UInt64?
    public var priority: PrioritySettings?
    public var urgency: UrgencySettings?

    public init(
        name: String, maxLeases: UInt32? = nil, maxPending: UInt64? = nil, overflow: String? = nil,
        retry: Retry? = nil, maxReleases: UInt32? = nil, leaseTtlMs: UInt64? = nil,
        priority: PrioritySettings? = nil, urgency: UrgencySettings? = nil
    ) {
        self.name = name
        self.maxLeases = maxLeases
        self.maxPending = maxPending
        self.overflow = overflow
        self.retry = retry
        self.maxReleases = maxReleases
        self.leaseTtlMs = leaseTtlMs
        self.priority = priority
        self.urgency = urgency
    }
}

/// A limit that tasks share: a semaphore or a rate limit.
public struct Resource: Codable, Sendable {
    public var kind: String
    public var name: String
    public var permits: UInt32?
    public var rate: Double?
    public var burst: UInt32?

    /// At most `permits` in use at a time.
    public static func semaphore(_ name: String, permits: UInt32) -> Resource {
        Resource(kind: "semaphore", name: name, permits: permits, rate: nil, burst: nil)
    }

    /// `rate` per second, with bursts of up to `burst`.
    public static func rate(_ name: String, perSecond rate: Double, burst: UInt32) -> Resource {
        Resource(kind: "rate", name: name, permits: nil, rate: rate, burst: burst)
    }
}

/// Where tasks are kept. Without one they are kept in memory.
public struct Store: Codable, Sendable {
    /// The path of a SQLite file. The library must be built with the SQLite store.
    public var sqlite: String?

    public init(sqlite: String? = nil) {
        self.sqlite = sqlite
    }
}

/// The configuration of a scheduler. The default is one pool named "default".
public struct Config: Codable, Sendable {
    public var pools: [Pool]?
    public var resources: [Resource]?
    public var priority: PrioritySettings?
    public var urgency: UrgencySettings?
    public var store: Store?

    public init(
        pools: [Pool]? = nil, resources: [Resource]? = nil, priority: PrioritySettings? = nil,
        urgency: UrgencySettings? = nil, store: Store? = nil
    ) {
        self.pools = pools
        self.resources = resources
        self.priority = priority
        self.urgency = urgency
        self.store = store
    }
}

/// The options of a submission. The default is a normal task in the pool "default".
public struct TaskOptions: Codable, Sendable {
    enum CodingKeys: String, CodingKey {
        case pool
        case kind
        case key
        case priority
        case deadlineMs = "deadline_ms"
        case timeoutMs = "timeout_ms"
        case retry
        case resources
        case tags
        case orderingKey = "ordering_key"
        case onFailure = "on_failure"
        case onKeyConflict = "on_key_conflict"
        case fingerprint
        case renewPosition = "renew_position"
        case maxReleases = "max_releases"
    }

    public var pool: String?
    public var kind: String?
    public var key: String?
    public var priority: Priority?
    /// A deadline that far from now.
    public var deadlineMs: UInt64?
    /// Ends an attempt that runs longer.
    public var timeoutMs: UInt64?
    public var retry: Retry?
    public var resources: [String: UInt32]?
    public var tags: [String: String]?
    /// Tasks with one ordering key run one at a time, in submission order.
    public var orderingKey: String?
    /// "continue" (default) or "halt": what a failed task does to the tasks behind it in
    /// its ordering key.
    public var onFailure: String?
    /// "return_existing" (default), "reject" or "replace_queued": what a submission does
    /// when a live task has its key.
    public var onKeyConflict: String?
    /// 1 to 32 hexadecimal digits. With `onKeyConflict` "reject", a submission whose key is
    /// taken by a task with another fingerprint is a conflict.
    public var fingerprint: String?
    /// Makes a replacement go to the back of the queue.
    public var renewPosition: Bool?
    /// Overrides the pool's limit for this task.
    public var maxReleases: UInt32?

    public init(
        pool: String? = nil, kind: String? = nil, key: String? = nil, priority: Priority? = nil,
        deadlineMs: UInt64? = nil, timeoutMs: UInt64? = nil, retry: Retry? = nil,
        resources: [String: UInt32]? = nil, tags: [String: String]? = nil,
        orderingKey: String? = nil, onFailure: String? = nil, onKeyConflict: String? = nil,
        fingerprint: String? = nil, renewPosition: Bool? = nil, maxReleases: UInt32? = nil
    ) {
        self.pool = pool
        self.kind = kind
        self.key = key
        self.priority = priority
        self.deadlineMs = deadlineMs
        self.timeoutMs = timeoutMs
        self.retry = retry
        self.resources = resources
        self.tags = tags
        self.orderingKey = orderingKey
        self.onFailure = onFailure
        self.onKeyConflict = onKeyConflict
        self.fingerprint = fingerprint
        self.renewPosition = renewPosition
        self.maxReleases = maxReleases
    }
}

/// What a submission did.
public enum Outcome: Int32, Sendable {
    /// A new task.
    case created = 0
    /// A live task with this key exists and is returned.
    case existing = 1
    /// The queued task with this key was replaced.
    case replaced = 2
    /// The task replaces a running one when that ends.
    case pending = 3
}

/// The result of a submission.
public struct Submitted: Sendable, Equatable {
    public let id: UInt64
    public let outcome: Outcome
}

/// Names a task: by key or by id.
public enum TaskRef: Sendable {
    case id(UInt64)
    case key(String)
}

/// What to change on a queued task. Nil fields are left alone.
public struct Update: Codable, Sendable {
    enum CodingKeys: String, CodingKey {
        case priority
        case deadlineMs = "deadline_ms"
        case noDeadline = "no_deadline"
        case tags
    }

    public var priority: Priority?
    /// A deadline that far from now.
    public var deadlineMs: UInt64?
    /// Removes the deadline.
    public var noDeadline: Bool?
    /// Replaces the tags.
    public var tags: [String: String]?

    public init(
        priority: Priority? = nil, deadlineMs: UInt64? = nil, noDeadline: Bool? = nil,
        tags: [String: String]? = nil
    ) {
        self.priority = priority
        self.deadlineMs = deadlineMs
        self.noDeadline = noDeadline
        self.tags = tags
    }
}

/// What to pause or resume: a whole pool, one class of it, or one ordering key of it.
public enum PauseTarget: Sendable {
    case pool
    case priorityClass(Priority)
    case orderingKey(String)
}

/// A task as the scheduler sees it.
public struct TaskInfo: Codable, Sendable {
    public let id: UInt64
    public let key: String?
    public let pool: String
    public let kind: String
    public let priority: Priority
    public let status: String
    public let attempt: UInt32
}

/// The ordering keys of a pool.
public struct OrderingSnapshot: Codable, Sendable {
    public let keys: UInt64
    public let waiting: UInt64
    public let halted: UInt64
    public let paused: UInt64
}

/// The state of one pool.
public struct PoolSnapshot: Codable, Sendable {
    enum CodingKeys: String, CodingKey {
        case name
        case capacity
        case busy
        case ready
        case pending
        case paused
        case pausedClasses = "paused_classes"
        case pausedTasks = "paused_tasks"
        case ordering
    }

    public let name: String
    public let capacity: UInt32
    public let busy: UInt32
    public let ready: UInt64
    /// Queued tasks per class name.
    public let pending: [String: UInt64]
    /// The reason the pool is paused with, or nil.
    public let paused: String?
    /// The reasons of the classes that are paused on their own, by class name.
    public let pausedClasses: [String: String]
    public let pausedTasks: UInt64
    public let ordering: OrderingSnapshot
}

/// The state of the scheduler.
public struct Snapshot: Codable, Sendable {
    enum CodingKeys: String, CodingKey {
        case pools
        case liveTasks = "live_tasks"
        case running
        case pending
        case totals
    }

    public let pools: [PoolSnapshot]
    public let liveTasks: UInt64
    public let running: UInt64
    public let pending: UInt64
    /// Counters since start: submitted, dispatched, succeeded, failed, dead_lettered,
    /// cancelled, shed, retries, timeouts, lease_expiries, superseded, releases.
    public let totals: [String: UInt64]
}

private func json<T: Encodable>(_ value: T) throws -> String {
    do {
        return String(decoding: try JSONEncoder().encode(value), as: UTF8.self)
    } catch {
        throw CronoqError.invalidArgument("\(error)")
    }
}

/// Turn a status of the C API into an error. The message is read at once, on the calling
/// thread, where the library keeps it.
private func check(_ status: cronoq_status) throws {
    if status == CRONOQ_OK {
        return
    }
    let message = String(cString: cronoq_last_error())
    switch status {
    case CRONOQ_TIMEOUT: throw CronoqError.timeout
    case CRONOQ_PAUSED: throw CronoqError.paused(reason: message)
    case CRONOQ_SHUTDOWN: throw CronoqError.shutdown
    case CRONOQ_INVALID_ARGUMENT: throw CronoqError.invalidArgument(message)
    case CRONOQ_NOT_FOUND: throw CronoqError.notFound(message)
    case CRONOQ_LEASE_GONE: throw CronoqError.leaseGone
    case CRONOQ_FULL: throw CronoqError.full(message)
    case CRONOQ_CONFLICT: throw CronoqError.conflict(message)
    case CRONOQ_STORE: throw CronoqError.store(message)
    default: throw CronoqError.internal(message)
    }
}

/// Call `body` with an optional C string.
private func withOptionalCString<R>(_ s: String?, _ body: (UnsafePointer<CChar>?) throws -> R) rethrows -> R {
    guard let s = s else { return try body(nil) }
    return try s.withCString { try body($0) }
}

private func milliseconds(_ t: TimeInterval) -> UInt64 {
    // Longer than a thousand years is "for ever"; this also keeps infinity from trapping.
    t > 0 ? UInt64(min(t, 3.0e10) * 1000) : 0
}

/// One attempt of a task, taken with `Scheduler.next`. End it with exactly one of
/// `complete`, `fail` and `release`.
public struct Lease: @unchecked Sendable {
    let scheduler: Scheduler
    /// Names the attempt.
    public let id: UInt64
    /// The task's id.
    public let task: UInt64
    /// 1 for the first attempt.
    public let attempt: UInt32
    /// What the task was submitted with.
    public let payload: Data
    /// The task's key.
    public let key: String?
    /// The task's kind, or "".
    public let kind: String
    /// The task's pool.
    public let pool: String
    /// The task's tags.
    public let tags: [String: String]
    /// The task's deadline.
    public let deadline: Date?
    /// The attempt's timeout.
    public let timeout: TimeInterval?

    /// Report that the attempt succeeded.
    public func complete() throws {
        try check(cronoq_complete(scheduler.handle, id))
    }

    /// Report that the attempt failed. A retryable failure is tried again after the task's
    /// backoff, and not before `retryAfter` has passed if given, until the retry policy is
    /// used up. A failure that is not retryable ends the task.
    public func fail(_ message: String, retryable: Bool = true, retryAfter: TimeInterval? = nil) throws {
        let after: Int64 = retryAfter.map { Int64(milliseconds($0)) } ?? -1
        try check(message.withCString { cronoq_fail(scheduler.handle, id, $0, retryable ? 1 : 0, after) })
    }

    /// Give the attempt back without a verdict: the program cannot run the task now, and
    /// nothing is wrong with the task. The task keeps its place and is handed out again
    /// after `delay`. This does not use up a retry, up to the pool's or the task's
    /// `maxReleases`.
    public func release(after delay: TimeInterval = 0) throws {
        try check(cronoq_release(scheduler.handle, id, milliseconds(delay)))
    }

    /// Extend the lease (needed for pools with `leaseTtlMs`). `CronoqError.leaseGone` means
    /// the lease is over, for example because its task was cancelled: stop working on it.
    public func heartbeat() throws {
        try check(cronoq_heartbeat(scheduler.handle, id))
    }
}

private struct LeaseInfo: Decodable {
    enum CodingKeys: String, CodingKey {
        case pool
        case tags
        case deadlineUnixMs = "deadline_unix_ms"
        case timeoutMs = "timeout_ms"
    }

    let pool: String
    let tags: [String: String]
    let deadlineUnixMs: Int64?
    let timeoutMs: Int64?
}

/// An embedded scheduler. It may be used from any number of threads at once. It is stopped
/// and released when the last reference to it goes away.
public final class Scheduler: @unchecked Sendable {
    let handle: OpaquePointer

    /// The version of the library.
    public static var libraryVersion: String {
        String(cString: cronoq_version())
    }

    /// Start a scheduler.
    public init(_ config: Config = Config()) throws {
        let got = cronoq_abi_version()
        guard got == abiVersion else {
            throw CronoqError.internal("the library implements ABI \(got), this package needs \(abiVersion)")
        }
        var out: OpaquePointer?
        try check(try json(config).withCString { cronoq_new($0, &out) })
        guard let handle = out else {
            throw CronoqError.internal("no scheduler was returned")
        }
        self.handle = handle
    }

    deinit {
        // No other thread can be inside a call: a call keeps a reference to the scheduler.
        _ = cronoq_shutdown(handle, 0)
        cronoq_free(handle)
    }

    /// Submit a task with `payload`.
    @discardableResult
    public func submit(_ payload: Data, _ options: TaskOptions? = nil) throws -> Submitted {
        let text = try options.map { try json($0) }
        var id: UInt64 = 0
        var outcome: Int32 = 0
        let status: cronoq_status = payload.withUnsafeBytes { bytes in
            withOptionalCString(text) { opts in
                cronoq_submit(
                    handle, bytes.bindMemory(to: UInt8.self).baseAddress, bytes.count, opts, &id,
                    &outcome)
            }
        }
        try check(status)
        return Submitted(id: id, outcome: Outcome(rawValue: outcome) ?? .created)
    }

    /// Wait up to `timeout` seconds for an attempt of a task of `pool` (nil is "default")
    /// and take it. The scheduler chooses the task when `next` asks, so the order of the
    /// queue is decided as late as possible.
    ///
    /// Throws `CronoqError.timeout` when nothing became ready in time, `.paused` when in
    /// addition the pool is paused, and `.shutdown` after `shutdown`.
    public func next(pool: String? = nil, timeout: TimeInterval) throws -> Lease {
        var raw = cronoq_lease()
        try check(withOptionalCString(pool) { cronoq_next_lease(handle, $0, milliseconds(timeout), &raw) })
        defer { cronoq_lease_free(&raw) }
        let info: LeaseInfo
        do {
            info = try JSONDecoder().decode(LeaseInfo.self, from: Data(String(cString: raw.info_json).utf8))
        } catch {
            throw CronoqError.internal("lease info: \(error)")
        }
        return Lease(
            scheduler: self,
            id: raw.lease,
            task: raw.task,
            attempt: raw.attempt,
            payload: raw.payload.map { Data(bytes: $0, count: raw.payload_len) } ?? Data(),
            key: raw.key.map { String(cString: $0) },
            kind: String(cString: raw.kind),
            pool: info.pool,
            tags: info.tags,
            deadline: info.deadlineUnixMs.map { Date(timeIntervalSince1970: Double($0) / 1000) },
            timeout: info.timeoutMs.map { Double($0) / 1000 })
    }

    private func withRef<R>(_ ref: TaskRef, _ body: (UInt64, UnsafePointer<CChar>?) throws -> R) rethrows -> R {
        switch ref {
        case .id(let id): return try body(id, nil)
        case .key(let key): return try key.withCString { try body(0, $0) }
        }
    }

    /// Cancel a task. Returns whether a live task was cancelled.
    @discardableResult
    public func cancel(_ ref: TaskRef) throws -> Bool {
        var out: Int32 = 0
        try check(withRef(ref) { cronoq_cancel(handle, $0, $1, &out) })
        return out != 0
    }

    /// Change a queued task without losing its place or its age. Returns whether something
    /// changed.
    @discardableResult
    public func update(_ ref: TaskRef, _ update: Update) throws -> Bool {
        let text = try json(update)
        var out: Int32 = 0
        try check(withRef(ref) { id, key in text.withCString { cronoq_update(handle, id, key, $0, &out) } })
        return out != 0
    }

    /// Set the lowest class the tasks of an ordering key run at; nil removes the floor.
    /// With `persist` the floor stays when the key has no tasks left.
    public func setOrderingFloor(
        pool: String? = nil, orderingKey: String, floor: Priority?, persist: Bool = false
    ) throws {
        try check(
            withOptionalCString(pool) { p in
                orderingKey.withCString {
                    cronoq_set_ordering_floor(handle, p, $0, floor?.index ?? -1, persist ? 1 : 0)
                }
            })
    }

    private func withTarget<R>(
        _ target: PauseTarget, _ body: (Int32, UnsafePointer<CChar>?) throws -> R
    ) rethrows -> R {
        switch target {
        case .pool: return try body(-1, nil)
        case .priorityClass(let c): return try body(c.index, nil)
        case .orderingKey(let k): return try k.withCString { try body(-1, $0) }
        }
    }

    /// Hold back a pool, one class of it or one ordering key of it: its tasks stay queued
    /// and keep their place, and none is handed out until `resume`. The reason is required.
    /// Returns whether this paused something that was not paused.
    @discardableResult
    public func pause(_ target: PauseTarget = .pool, pool: String? = nil, reason: String) throws -> Bool {
        var out: Int32 = 0
        try check(
            withOptionalCString(pool) { p in
                withTarget(target) { cls, key in
                    reason.withCString { cronoq_pause(handle, p, cls, key, $0, &out) }
                }
            })
        return out != 0
    }

    /// End a pause. Resuming an ordering key also resumes a key that a failed task halted.
    /// Returns whether something was resumed.
    @discardableResult
    public func resume(_ target: PauseTarget = .pool, pool: String? = nil) throws -> Bool {
        var out: Int32 = 0
        try check(
            withOptionalCString(pool) { p in
                withTarget(target) { cls, key in cronoq_resume(handle, p, cls, key, &out) }
            })
        return out != 0
    }

    private func take(_ buf: inout cronoq_buf) -> Data {
        defer { cronoq_buf_free(&buf) }
        guard let ptr = buf.ptr else { return Data() }
        return Data(bytes: ptr, count: buf.len)
    }

    /// Describe a live task. Throws `CronoqError.notFound` if the scheduler does not know it.
    public func task(_ ref: TaskRef) throws -> TaskInfo {
        var buf = cronoq_buf()
        try check(withRef(ref) { cronoq_task(handle, $0, $1, &buf) })
        do {
            return try JSONDecoder().decode(TaskInfo.self, from: take(&buf))
        } catch {
            throw CronoqError.internal("\(error)")
        }
    }

    /// The state of the scheduler.
    public func snapshot() throws -> Snapshot {
        var buf = cronoq_buf()
        try check(cronoq_snapshot(handle, &buf))
        do {
            return try JSONDecoder().decode(Snapshot.self, from: take(&buf))
        } catch {
            throw CronoqError.internal("\(error)")
        }
    }

    /// Stop the scheduler: threads waiting in `next` throw `CronoqError.shutdown` at once
    /// and no new lease is handed out. For up to `grace` seconds, leases that are held can
    /// still be reported; the call returns as soon as all are. A lease that is still held
    /// after that is released: its task keeps its place and no retry is used.
    public func shutdown(grace: TimeInterval = 0) throws {
        try check(cronoq_shutdown(handle, milliseconds(grace)))
    }
}
