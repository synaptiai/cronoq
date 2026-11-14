import Foundation
import XCTest

@testable import Cronoq

final class CronoqTests: XCTestCase {
    private func submit(_ s: Scheduler, _ payload: String, _ options: TaskOptions? = nil) throws -> UInt64 {
        try s.submit(Data(payload.utf8), options).id
    }

    /// Take and complete leases until the queue is empty; their payloads in order.
    private func drain(_ s: Scheduler) throws -> [String] {
        var out: [String] = []
        while true {
            do {
                let lease = try s.next(timeout: 0.1)
                try lease.complete()
                out.append(String(decoding: lease.payload, as: UTF8.self))
            } catch CronoqError.timeout {
                return out
            }
        }
    }

    /// Reports are applied by the scheduler's thread shortly after the call returns.
    private func eventually(_ s: Scheduler, _ ok: (Snapshot) -> Bool) throws -> Snapshot {
        let deadline = Date().addingTimeInterval(5)
        while true {
            let snap = try s.snapshot()
            if ok(snap) { return snap }
            XCTAssertLessThan(Date(), deadline, "not reached: \(snap)")
            if Date() >= deadline { return snap }
            Thread.sleep(forTimeInterval: 0.002)
        }
    }

    func testVersion() {
        XCTAssertFalse(Scheduler.libraryVersion.isEmpty)
    }

    func testPriorityOrder() throws {
        let s = try Scheduler()
        _ = try submit(s, "low", TaskOptions(priority: .low))
        _ = try submit(s, "normal")
        _ = try submit(s, "critical", TaskOptions(priority: .critical))
        _ = try submit(s, "high", TaskOptions(priority: .high))
        XCTAssertEqual(try drain(s), ["critical", "high", "normal", "low"])
        let snap = try eventually(s) { $0.totals["succeeded"] == 4 }
        XCTAssertEqual(snap.pools[0].name, "default")
    }

    func testLeaseCarriesTheTask() throws {
        let s = try Scheduler()
        let id = try submit(
            s, "hello",
            TaskOptions(kind: "send", key: "k1", deadlineMs: 30000, timeoutMs: 60000, tags: ["camelCase": "b"]))
        let lease = try s.next(timeout: 5)
        XCTAssertEqual(lease.task, id)
        XCTAssertEqual(lease.attempt, 1)
        XCTAssertEqual(String(decoding: lease.payload, as: UTF8.self), "hello")
        XCTAssertEqual(lease.key, "k1")
        XCTAssertEqual(lease.kind, "send")
        XCTAssertEqual(lease.pool, "default")
        XCTAssertEqual(lease.tags["camelCase"], "b")
        XCTAssertEqual(lease.timeout, 60)
        XCTAssertNotNil(lease.deadline)
        let info = try s.task(.key("k1"))
        XCTAssertEqual(info.id, id)
        XCTAssertEqual(info.status, "running")
        XCTAssertEqual(info.priority, .normal)
        try lease.heartbeat()
        try lease.complete()
        XCTAssertThrowsError(try lease.complete()) { XCTAssertEqual($0 as? CronoqError, .leaseGone) }

        _ = try s.submit(Data())
        let empty = try s.next(timeout: 5)
        XCTAssertTrue(empty.payload.isEmpty)
        XCTAssertNil(empty.key)
        try empty.complete()
    }

    func testOrderingKeys() throws {
        let s = try Scheduler()
        _ = try submit(s, "a1", TaskOptions(priority: .low, orderingKey: "a"))
        _ = try submit(s, "a2", TaskOptions(priority: .low, orderingKey: "a"))
        _ = try submit(s, "n")
        try s.setOrderingFloor(orderingKey: "a", floor: .high)
        XCTAssertEqual(try drain(s), ["a1", "a2", "n"])
    }

    func testReplaceByKey() throws {
        let s = try Scheduler()
        let first = try submit(s, "v1", TaskOptions(key: "title"))
        let replaced = try s.submit(Data("v2".utf8), TaskOptions(key: "title", onKeyConflict: "replace_queued"))
        XCTAssertEqual(replaced, Submitted(id: first, outcome: .replaced))
        let existing = try s.submit(Data("v3".utf8), TaskOptions(key: "title"))
        XCTAssertEqual(existing, Submitted(id: first, outcome: .existing))
        _ = try submit(s, "f1", TaskOptions(key: "f", fingerprint: "0a"))
        XCTAssertThrowsError(
            try s.submit(Data("f2".utf8), TaskOptions(key: "f", onKeyConflict: "reject", fingerprint: "0b"))
        ) { error in
            guard case .conflict = error as? CronoqError else { return XCTFail("\(error)") }
        }
        XCTAssertEqual(try drain(s), ["v2", "f1"])
    }

    func testPauseAndResume() throws {
        let s = try Scheduler()
        _ = try submit(s, "x")
        XCTAssertTrue(try s.pause(reason: "relay deploy"))
        XCTAssertThrowsError(try s.next(timeout: 0.05)) {
            XCTAssertEqual($0 as? CronoqError, .paused(reason: "relay deploy"))
        }
        var snap = try s.snapshot()
        XCTAssertEqual(snap.pools[0].paused, "relay deploy")
        XCTAssertEqual(snap.pools[0].pausedTasks, 1)
        XCTAssertTrue(try s.resume())
        XCTAssertEqual(try drain(s), ["x"])

        _ = try submit(s, "low", TaskOptions(priority: .low))
        _ = try submit(s, "keyed", TaskOptions(orderingKey: "k"))
        _ = try submit(s, "free")
        try s.pause(.priorityClass(.low), reason: "backfill off")
        try s.pause(.orderingKey("k"), reason: "repair")
        XCTAssertEqual(try drain(s), ["free"])
        snap = try s.snapshot()
        XCTAssertEqual(snap.pools[0].pausedClasses["low"], "backfill off")
        XCTAssertEqual(snap.pools[0].ordering.paused, 1)
        try s.resume(.priorityClass(.low))
        try s.resume(.orderingKey("k"))
        XCTAssertEqual(try drain(s), ["keyed", "low"])
    }

    func testReleaseDoesNotUseARetry() throws {
        let s = try Scheduler(Config(pools: [Pool(name: "default", retry: Retry(maxAttempts: 1))]))
        let id = try submit(s, "x")
        try s.next(timeout: 5).release()
        let lease = try s.next(timeout: 5)
        XCTAssertEqual(lease.task, id)
        XCTAssertEqual(lease.attempt, 2)
        try lease.complete()
        let snap = try eventually(s) { $0.totals["succeeded"] == 1 }
        XCTAssertEqual(snap.totals["releases"], 1)
        XCTAssertEqual(snap.totals["retries"], 0)
    }

    func testFailUpdateAndCancel() throws {
        let s = try Scheduler(
            Config(pools: [Pool(name: "default", retry: Retry(maxAttempts: 2, initialBackoffMs: 1, jitter: false))]))
        let id = try submit(s, "x")
        try s.next(timeout: 5).fail("relay said no")
        let second = try s.next(timeout: 5)
        XCTAssertEqual(second.task, id)
        XCTAssertEqual(second.attempt, 2)
        try second.fail("for good", retryable: false)
        _ = try eventually(s) { $0.totals["failed"] == 1 }

        _ = try submit(s, "normal")
        let low = try submit(s, "was-low", TaskOptions(key: "k", priority: .low))
        let gone = try submit(s, "cancelled")
        XCTAssertTrue(try s.update(.key("k"), Update(priority: .critical)))
        XCTAssertEqual(try s.task(.id(low)).priority, .critical)
        XCTAssertTrue(try s.cancel(.id(gone)))
        XCTAssertFalse(try s.cancel(.id(gone)))
        XCTAssertEqual(try drain(s), ["was-low", "normal"])
        XCTAssertThrowsError(try s.task(.id(999_999))) { error in
            guard case .notFound = error as? CronoqError else { return XCTFail("\(error)") }
        }

        let running = try submit(s, "y")
        let lease = try s.next(timeout: 5)
        XCTAssertTrue(try s.cancel(.id(running)))
        XCTAssertThrowsError(try lease.heartbeat()) { XCTAssertEqual($0 as? CronoqError, .leaseGone) }
    }

    func testResourcesAndErrors() throws {
        let s = try Scheduler(
            Config(
                pools: [Pool(name: "default", maxPending: 2)],
                resources: [.semaphore("uplink", permits: 1)]))
        _ = try submit(s, "a", TaskOptions(resources: ["uplink": 1]))
        _ = try submit(s, "b", TaskOptions(resources: ["uplink": 1]))
        XCTAssertThrowsError(try s.submit(Data("c".utf8))) { error in
            guard case .full = error as? CronoqError else { return XCTFail("\(error)") }
        }
        let a = try s.next(timeout: 5)
        XCTAssertThrowsError(try s.next(timeout: 0.05)) { XCTAssertEqual($0 as? CronoqError, .timeout) }
        try a.complete()
        XCTAssertEqual(try drain(s), ["b"])
        XCTAssertThrowsError(try s.submit(Data(), TaskOptions(pool: "nope"))) { error in
            guard case .invalidArgument = error as? CronoqError else { return XCTFail("\(error)") }
        }
        XCTAssertThrowsError(try Scheduler(Config(pools: [Pool(name: "a"), Pool(name: "a")]))) { error in
            guard case .invalidArgument = error as? CronoqError else { return XCTFail("\(error)") }
        }
    }

    func testOutputAndWait() throws {
        let s = try Scheduler(Config(pools: [Pool(name: "default", retry: Retry(maxAttempts: 1))]))
        let id = try submit(s, "2+2", TaskOptions(key: "sum"))
        XCTAssertThrowsError(try s.wait(.id(id), timeout: 0.02)) { XCTAssertEqual($0 as? CronoqError, .timeout) }
        try s.next(timeout: 5).complete(output: Data("4".utf8))
        XCTAssertEqual(try s.wait(.key("sum"), timeout: 5), .succeeded(output: Data("4".utf8)))
        let bad = try submit(s, "y")
        try s.next(timeout: 5).fail("relay refused")
        XCTAssertEqual(try s.wait(.id(bad), timeout: 5), .ended("dead_lettered: relay refused"))
        XCTAssertThrowsError(try s.wait(.id(999_999), timeout: 0)) { error in
            guard case .notFound = error as? CronoqError else { return XCTFail("\(error)") }
        }
    }

    func testEvents() throws {
        let s = try Scheduler(Config(events: "all"))
        let events = try s.events()
        XCTAssertThrowsError(try events.next(timeout: 0.02)) { XCTAssertEqual($0 as? CronoqError, .timeout) }
        let id = try submit(s, "x", TaskOptions(key: "k", priority: .high))
        try s.next(timeout: 5).complete()
        var seen: [String] = []
        while let event = try? events.next(timeout: 0.3) {
            XCTAssertEqual(event.task, id)
            XCTAssertEqual(event.key, "k")
            XCTAssertEqual(event.pool, "default")
            XCTAssertEqual(event.priority, .high)
            seen.append("\(event.kind ?? ""):\(event.status ?? "")")
        }
        XCTAssertEqual(seen.last, "transition:succeeded")
        XCTAssertGreaterThanOrEqual(seen.count, 2)
        try s.shutdown()
        XCTAssertThrowsError(try events.next(timeout: 1)) { XCTAssertEqual($0 as? CronoqError, .shutdown) }
    }

    func testClockAfterAGap() throws {
        let s = try Scheduler(
            Config(pools: [
                Pool(
                    name: "default",
                    retry: Retry(maxAttempts: 2, initialBackoffMs: 3_600_000, maxBackoffMs: 3_600_000, jitter: false))
            ]))
        let id = try submit(s, "x")
        try s.next(timeout: 5).fail("later")
        XCTAssertThrowsError(try s.next(timeout: 0.05)) { XCTAssertEqual($0 as? CronoqError, .timeout) }
        XCTAssertLessThan(try s.resyncClock(), 1)
        try s.advanceClock(by: 3600)
        let lease = try s.next(timeout: 2)
        XCTAssertEqual(lease.task, id)
        XCTAssertEqual(lease.attempt, 2)
        try lease.complete()
        XCTAssertEqual(try s.resyncClock(), 0)
    }

    func testShutdownWakesWaiters() throws {
        let s = try Scheduler()
        let woken = expectation(description: "the waiting thread is woken")
        Thread.detachNewThread {
            do {
                _ = try s.next(timeout: 60)
                XCTFail("got a lease")
            } catch {
                XCTAssertEqual(error as? CronoqError, .shutdown)
            }
            woken.fulfill()
        }
        Thread.sleep(forTimeInterval: 0.05)
        try s.shutdown()
        wait(for: [woken], timeout: 5)
        XCTAssertThrowsError(try s.submit(Data())) { XCTAssertEqual($0 as? CronoqError, .shutdown) }
    }
}
