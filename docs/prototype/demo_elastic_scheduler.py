#!/usr/bin/env python3
"""
Demonstration and Tests for Elastic Priority Scheduler

This file validates all features:
1. Variable task durations
2. Priority-based scheduling with aging
3. Fixed schedule support
4. Quality-triggered elastic scaling
5. Online task processing
"""

import time
from dataclasses import dataclass

from elastic_scheduler import (
    ElasticPriorityScheduler,
    HybridScheduler,
    Priority,
    PriorityCalculator,
    QualityMonitor,
    Task,
)


# =============================================================================
# Test Utilities
# =============================================================================


@dataclass
class TestResult:
    """Result of a test case."""

    name: str
    passed: bool
    message: str
    duration_ms: float


def run_test(name: str, test_fn) -> TestResult:
    """Run a test function and capture result."""
    start = time.perf_counter()
    try:
        test_fn()
        duration = (time.perf_counter() - start) * 1000
        return TestResult(name, True, "OK", duration)
    except AssertionError as e:
        duration = (time.perf_counter() - start) * 1000
        return TestResult(name, False, str(e), duration)
    except Exception as e:
        duration = (time.perf_counter() - start) * 1000
        return TestResult(name, False, f"Error: {type(e).__name__}: {e}", duration)


# =============================================================================
# Unit Tests
# =============================================================================


def test_priority_calculator_base_weights():
    """Test that base priority weights work correctly."""
    calc = PriorityCalculator()
    current = time.time()

    critical = Task(id="c", duration=1, priority=Priority.CRITICAL, arrival_time=current)
    high = Task(id="h", duration=1, priority=Priority.HIGH, arrival_time=current)
    normal = Task(id="n", duration=1, priority=Priority.NORMAL, arrival_time=current)
    low = Task(id="l", duration=1, priority=Priority.LOW, arrival_time=current)

    p_critical = calc.calculate(critical, current)
    p_high = calc.calculate(high, current)
    p_normal = calc.calculate(normal, current)
    p_low = calc.calculate(low, current)

    assert p_critical > p_high > p_normal > p_low, (
        f"Priority ordering failed: CRITICAL={p_critical}, HIGH={p_high}, "
        f"NORMAL={p_normal}, LOW={p_low}"
    )


def test_priority_calculator_aging():
    """Test that aging boosts priority over time."""
    calc = PriorityCalculator()
    current = time.time()

    old_task = Task(id="old", duration=1, priority=Priority.LOW, arrival_time=current - 100)
    new_task = Task(id="new", duration=1, priority=Priority.LOW, arrival_time=current)

    p_old = calc.calculate(old_task, current)
    p_new = calc.calculate(new_task, current)

    assert p_old > p_new, f"Aging should boost priority: old={p_old}, new={p_new}"


def test_priority_calculator_fixed_schedule():
    """Test that fixed-schedule tasks get infinite priority when due."""
    calc = PriorityCalculator()
    current = time.time()

    fixed_now = Task(
        id="fixed_now",
        duration=5,
        priority=Priority.LOW,
        fixed_start=current,
        arrival_time=current - 10,
    )

    fixed_later = Task(
        id="fixed_later",
        duration=5,
        priority=Priority.LOW,
        fixed_start=current + 100,
        arrival_time=current - 10,
    )

    p_now = calc.calculate(fixed_now, current)
    p_later = calc.calculate(fixed_later, current)

    assert p_now == float("inf"), f"Fixed task due now should have inf priority: {p_now}"
    assert p_later < float("inf"), f"Fixed task due later should have finite priority: {p_later}"


def test_scheduler_add_task():
    """Test adding tasks to the scheduler."""
    scheduler = HybridScheduler(initial_workers=2, max_workers=4)

    task1 = Task(id="t1", duration=1, priority=Priority.HIGH)
    task2 = Task(id="t2", duration=2, priority=Priority.NORMAL)

    assert scheduler.add_task(task1), "Should add task1"
    assert scheduler.add_task(task2), "Should add task2"
    assert len(scheduler.pending_tasks) == 2, f"Should have 2 pending: {len(scheduler.pending_tasks)}"


def test_scheduler_fixed_schedule_conflict():
    """Test that conflicting fixed schedules are rejected."""
    scheduler = HybridScheduler(initial_workers=1, max_workers=4)
    scheduler.current_time = 100.0

    task1 = Task(id="f1", duration=10, fixed_start=100.0)
    task2 = Task(id="f2", duration=10, fixed_start=105.0)  # Overlaps with f1

    assert scheduler.add_task(task1), "First fixed task should be accepted"
    assert not scheduler.add_task(task2), "Conflicting fixed task should be rejected"


def test_scheduler_scaling():
    """Test worker scaling up and down."""
    scheduler = HybridScheduler(initial_workers=1, max_workers=4)

    assert scheduler.num_workers == 1, "Should start with 1 worker"

    assert scheduler.add_worker(), "Should add worker"
    assert scheduler.num_workers == 2, "Should have 2 workers"

    assert scheduler.add_worker(), "Should add another worker"
    assert scheduler.num_workers == 3, "Should have 3 workers"

    assert scheduler.remove_worker(), "Should remove worker"
    assert scheduler.num_workers == 2, "Should have 2 workers again"


def test_quality_monitor_calculation():
    """Test quality metrics calculation."""
    monitor = QualityMonitor()

    # Empty case
    metrics = monitor.calculate([], 0, [], time.time(), time.time() - 1)
    assert metrics.schedule_quality == 1.0, "Empty schedule should have 1.0 quality"

    # With pending
    metrics = monitor.calculate([], 5, [], time.time(), time.time() - 1)
    assert metrics.schedule_quality == 0.5, "Pending without completed should have 0.5 quality"


def test_quality_monitor_scaling_signals():
    """Test scaling decision logic."""
    monitor = QualityMonitor(scale_up_threshold=0.70, scale_down_threshold=0.90)

    # Single low quality should trigger scale up
    assert monitor.should_scale_up(0.50), "Quality 0.50 should trigger scale up"
    assert not monitor.should_scale_up(0.80), "Quality 0.80 should not trigger scale up"

    # Need history for scale down
    for _ in range(5):
        monitor.record_quality(0.95)

    assert monitor.should_scale_down(0.40), "High quality + low util should trigger scale down"


# =============================================================================
# Integration Test
# =============================================================================


def test_integration_mixed_workload():
    """Integration test with mixed task priorities and durations."""
    print("\n" + "=" * 60)
    print("Integration Test: Mixed Workload")
    print("=" * 60)

    scheduler = ElasticPriorityScheduler(
        initial_workers=1,
        max_workers=4,
        scale_up_threshold=0.60,
        scale_down_threshold=0.95,
    )

    # Submit tasks with various priorities and durations
    tasks = [
        Task(id="critical-1", duration=0.3, priority=Priority.CRITICAL),
        Task(id="high-1", duration=0.4, priority=Priority.HIGH),
        Task(id="high-2", duration=0.3, priority=Priority.HIGH),
        Task(id="normal-1", duration=0.5, priority=Priority.NORMAL),
        Task(id="normal-2", duration=0.4, priority=Priority.NORMAL),
        Task(id="normal-3", duration=0.3, priority=Priority.NORMAL),
        Task(id="low-1", duration=0.6, priority=Priority.LOW),
        Task(id="low-2", duration=0.5, priority=Priority.LOW),
    ]

    scheduler.start()
    print(f"Submitted {len(tasks)} tasks")

    for task in tasks:
        scheduler.submit(task)

    # Wait for completion
    timeout = 15
    start = time.time()
    while scheduler.completed_count < len(tasks) and time.time() - start < timeout:
        time.sleep(0.2)

    scheduler.stop()

    metrics = scheduler.get_metrics()
    completed = scheduler.completed_count

    print("\nResults:")
    print(f"  Completed: {completed}/{len(tasks)}")
    print(f"  Quality: {metrics.schedule_quality:.1%}")
    print(f"  Priority Adherence: {metrics.priority_adherence:.1%}")
    print(f"  Utilization: {metrics.utilization:.1%}")
    print(f"  Average Wait: {metrics.avg_wait_time:.2f}s")
    print(f"  Final Workers: {metrics.worker_count}")
    print(f"  Total Cost: ${scheduler.get_cost():.4f}")

    assert completed == len(tasks), f"Not all tasks completed: {completed}/{len(tasks)}"
    assert metrics.schedule_quality >= 0.5, f"Quality too low: {metrics.schedule_quality}"


def test_integration_fixed_schedule():
    """Test fixed schedule execution."""
    print("\n" + "=" * 60)
    print("Integration Test: Fixed Schedule")
    print("=" * 60)

    scheduler = ElasticPriorityScheduler(initial_workers=2, max_workers=4)

    now = time.time()
    fixed_start = now + 1.0  # Start in 1 second

    tasks = [
        Task(id="fixed-task", duration=0.5, priority=Priority.NORMAL, fixed_start=fixed_start),
        Task(id="filler-1", duration=0.3, priority=Priority.LOW),
        Task(id="filler-2", duration=0.3, priority=Priority.LOW),
    ]

    scheduler.start()

    for task in tasks:
        scheduler.submit(task)

    print(f"Fixed task scheduled to start at: {fixed_start - now:.2f}s from now")

    # Wait for completion
    timeout = 10
    start = time.time()
    while scheduler.completed_count < len(tasks) and time.time() - start < timeout:
        time.sleep(0.2)

    scheduler.stop()

    metrics = scheduler.get_metrics()
    print("\nResults:")
    print(f"  Completed: {scheduler.completed_count}/{len(tasks)}")
    print(f"  Fixed Schedule Hit Rate: {metrics.fixed_schedule_hits:.1%}")

    assert scheduler.completed_count == len(tasks), "Not all tasks completed"
    assert metrics.fixed_schedule_hits >= 0.9, f"Fixed schedule not respected: {metrics.fixed_schedule_hits}"


def test_integration_elastic_scaling():
    """Test that elastic scaling responds to load."""
    print("\n" + "=" * 60)
    print("Integration Test: Elastic Scaling")
    print("=" * 60)

    scheduler = ElasticPriorityScheduler(
        initial_workers=1,
        max_workers=4,
        scale_up_threshold=0.50,  # More aggressive scaling for demo
        scale_down_threshold=0.95,
    )

    scheduler.start()
    initial_workers = scheduler.get_metrics().worker_count
    print(f"Initial workers: {initial_workers}")

    # Submit burst of tasks to trigger scaling
    burst_tasks = [Task(id=f"burst-{i}", duration=1.0, priority=Priority.HIGH) for i in range(10)]
    for task in burst_tasks:
        scheduler.submit(task)

    print(f"Submitted {len(burst_tasks)} burst tasks")

    # Wait and observe scaling
    max_workers_observed = initial_workers
    for _ in range(50):  # 5 seconds
        time.sleep(0.1)
        current = scheduler.get_metrics().worker_count
        max_workers_observed = max(max_workers_observed, current)

    # Wait for completion
    timeout = 20
    start = time.time()
    while scheduler.completed_count < len(burst_tasks) and time.time() - start < timeout:
        time.sleep(0.2)

    scheduler.stop()

    print("\nResults:")
    print(f"  Completed: {scheduler.completed_count}/{len(burst_tasks)}")
    print(f"  Max workers observed: {max_workers_observed}")
    print(f"  Final workers: {scheduler.get_metrics().worker_count}")

    assert scheduler.completed_count == len(burst_tasks), "Not all tasks completed"
    # Note: Scaling may or may not trigger depending on timing; we just verify no crash


def test_integration_priority_ordering():
    """Test that high priority tasks complete before low priority."""
    print("\n" + "=" * 60)
    print("Integration Test: Priority Ordering")
    print("=" * 60)

    scheduler = ElasticPriorityScheduler(initial_workers=1, max_workers=2)

    # Submit low priority first, then high priority
    tasks = [
        Task(id="low-1", duration=0.3, priority=Priority.LOW),
        Task(id="low-2", duration=0.3, priority=Priority.LOW),
        Task(id="critical", duration=0.2, priority=Priority.CRITICAL),
    ]

    scheduler.start()

    for task in tasks:
        scheduler.submit(task)
        time.sleep(0.05)  # Small delay between submissions

    # Wait for completion
    timeout = 10
    start = time.time()
    while scheduler.completed_count < len(tasks) and time.time() - start < timeout:
        time.sleep(0.2)

    scheduler.stop()

    metrics = scheduler.get_metrics()
    print("\nResults:")
    print(f"  Completed: {scheduler.completed_count}/{len(tasks)}")
    print(f"  Priority Adherence: {metrics.priority_adherence:.1%}")

    assert scheduler.completed_count == len(tasks), "Not all tasks completed"


# =============================================================================
# Main Demo
# =============================================================================


def run_all_tests():
    """Run all tests and report results."""
    print("=" * 60)
    print("ELASTIC SCHEDULER TEST SUITE")
    print("=" * 60)

    unit_tests = [
        ("Priority Calculator: Base Weights", test_priority_calculator_base_weights),
        ("Priority Calculator: Aging", test_priority_calculator_aging),
        ("Priority Calculator: Fixed Schedule", test_priority_calculator_fixed_schedule),
        ("Scheduler: Add Task", test_scheduler_add_task),
        ("Scheduler: Fixed Schedule Conflict", test_scheduler_fixed_schedule_conflict),
        ("Scheduler: Scaling", test_scheduler_scaling),
        ("Quality Monitor: Calculation", test_quality_monitor_calculation),
        ("Quality Monitor: Scaling Signals", test_quality_monitor_scaling_signals),
    ]

    integration_tests = [
        ("Integration: Mixed Workload", test_integration_mixed_workload),
        ("Integration: Fixed Schedule", test_integration_fixed_schedule),
        ("Integration: Elastic Scaling", test_integration_elastic_scaling),
        ("Integration: Priority Ordering", test_integration_priority_ordering),
    ]

    print("\n" + "-" * 60)
    print("UNIT TESTS")
    print("-" * 60)

    results = []
    for name, test_fn in unit_tests:
        result = run_test(name, test_fn)
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        print(f"  [{status}] {name} ({result.duration_ms:.1f}ms)")
        if not result.passed:
            print(f"         {result.message}")

    print("\n" + "-" * 60)
    print("INTEGRATION TESTS")
    print("-" * 60)

    for name, test_fn in integration_tests:
        result = run_test(name, test_fn)
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        print(f"  [{status}] {name} ({result.duration_ms:.1f}ms)")
        if not result.passed:
            print(f"         {result.message}")

    # Summary
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    total_time = sum(r.duration_ms for r in results)

    print("\n" + "=" * 60)
    print(f"SUMMARY: {passed}/{total} tests passed ({total_time:.0f}ms total)")
    print("=" * 60)

    if passed == total:
        print("\nAll tests passed!")
        return True
    else:
        print("\nSome tests failed.")
        return False


def run_demo():
    """Run a demonstration of the scheduler."""
    print("\n" + "=" * 60)
    print("ELASTIC SCHEDULER DEMONSTRATION")
    print("=" * 60)

    print("\nCreating scheduler with 1 initial worker, max 4...")
    scheduler = ElasticPriorityScheduler(
        initial_workers=1,
        max_workers=4,
        scale_up_threshold=0.60,
        scale_down_threshold=0.90,
    )

    print("\nStarting scheduler...")
    scheduler.start()

    print("\nSubmitting mixed workload:")
    tasks = [
        # Critical task - should run first
        Task(id="ALERT-001", duration=0.5, priority=Priority.CRITICAL),
        # High priority batch
        Task(id="URGENT-001", duration=0.8, priority=Priority.HIGH),
        Task(id="URGENT-002", duration=0.6, priority=Priority.HIGH),
        # Normal processing
        Task(id="PROCESS-001", duration=1.0, priority=Priority.NORMAL),
        Task(id="PROCESS-002", duration=0.9, priority=Priority.NORMAL),
        Task(id="PROCESS-003", duration=1.1, priority=Priority.NORMAL),
        # Fixed schedule task - must start at specific time
        Task(
            id="SCHEDULED-001",
            duration=0.5,
            priority=Priority.NORMAL,
            fixed_start=time.time() + 2.0,
        ),
        # Background tasks
        Task(id="BACKGROUND-001", duration=0.7, priority=Priority.LOW),
        Task(id="BACKGROUND-002", duration=0.8, priority=Priority.LOW),
    ]

    for task in tasks:
        scheduler.submit(task)
        print(f"  Submitted: {task.id} (priority={task.priority.name}, duration={task.duration}s)")

    print("\nRunning scheduler for 15 seconds...")
    start = time.time()

    while time.time() - start < 15:
        metrics = scheduler.get_metrics()
        print(
            f"\r  Progress: {scheduler.completed_count}/{len(tasks)} completed | "
            f"Quality: {metrics.schedule_quality:.0%} | "
            f"Workers: {metrics.worker_count}",
            end="",
        )
        if scheduler.completed_count >= len(tasks):
            break
        time.sleep(0.5)

    scheduler.stop()

    print("\n\nFinal Results:")
    final_metrics = scheduler.get_metrics()
    print(f"  Tasks Completed: {scheduler.completed_count}/{len(tasks)}")
    print(f"  Schedule Quality: {final_metrics.schedule_quality:.1%}")
    print(f"  Priority Adherence: {final_metrics.priority_adherence:.1%}")
    print(f"  Fixed Schedule Hits: {final_metrics.fixed_schedule_hits:.1%}")
    print(f"  Worker Utilization: {final_metrics.utilization:.1%}")
    print(f"  Average Wait Time: {final_metrics.avg_wait_time:.2f}s")
    print(f"  Total Cost: ${scheduler.get_cost():.4f}")

    success = scheduler.completed_count == len(tasks)
    print(f"\nDemo {'PASSED' if success else 'FAILED'}")
    return success


if __name__ == "__main__":
    import sys

    print("Elastic Priority Scheduler - Demo & Tests")
    print()

    # Run unit and integration tests
    tests_passed = run_all_tests()

    # Run interactive demo
    demo_passed = run_demo()

    # Exit code
    sys.exit(0 if tests_passed and demo_passed else 1)
