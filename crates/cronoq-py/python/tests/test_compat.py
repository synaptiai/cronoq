"""The prototype's demo tests (docs/prototype/demo_elastic_scheduler.py), ported to the compat shim.

Unit tests that poked at prototype internals (PriorityCalculator, HybridScheduler,
QualityMonitor) are expressed as equivalent behavioural checks; integration tests keep their
original scenarios and assertions.
"""

import time

from cronoq.compat import ElasticPriorityScheduler, Priority, Task


def wait_for(scheduler, n, timeout):
    start = time.time()
    while scheduler.completed_count < n and time.time() - start < timeout:
        time.sleep(0.05)


def start_order(scheduler):
    return [s.task.id for s in sorted(scheduler.completed_tasks, key=lambda s: s.start_time)]


# ----------------------------------------------------------------------------- "unit" tests


def test_priority_base_weights_order_dispatch():
    s = ElasticPriorityScheduler(initial_workers=1, max_workers=1)
    s.start()
    s.submit(Task(id="blocker", duration=0.3))
    for tid, p in [("l", Priority.LOW), ("n", Priority.NORMAL), ("h", Priority.HIGH), ("c", Priority.CRITICAL)]:
        s.submit(Task(id=tid, duration=0.05, priority=p))
    wait_for(s, 5, 10)
    s.stop()
    assert start_order(s) == ["blocker", "c", "h", "n", "l"]


def test_aging_older_task_of_same_class_runs_first():
    s = ElasticPriorityScheduler(initial_workers=1, max_workers=1)
    s.start()
    s.submit(Task(id="blocker", duration=0.3))
    s.submit(Task(id="old", duration=0.05, priority=Priority.LOW))
    time.sleep(0.1)
    s.submit(Task(id="new", duration=0.05, priority=Priority.LOW))
    wait_for(s, 3, 10)
    s.stop()
    assert start_order(s) == ["blocker", "old", "new"]


def test_fixed_schedule_due_task_preempts_queue_but_later_one_waits():
    s = ElasticPriorityScheduler(initial_workers=1, max_workers=1)
    s.start()
    now = time.time()
    s.submit(Task(id="blocker", duration=0.5))
    s.submit(Task(id="low-1", duration=0.05, priority=Priority.LOW))
    s.submit(Task(id="fixed_now", duration=0.1, priority=Priority.LOW, fixed_start=now + 0.2))
    s.submit(Task(id="fixed_later", duration=0.1, priority=Priority.LOW, fixed_start=now + 100))
    wait_for(s, 3, 10)
    s.stop()
    order = start_order(s)
    # Due fixed task runs as soon as a worker frees up, ahead of queued work.
    assert order[:3] == ["blocker", "fixed_now", "low-1"]
    assert "fixed_later" not in order


def test_scheduler_add_task_before_start():
    s = ElasticPriorityScheduler(initial_workers=2, max_workers=4)
    assert s.submit(Task(id="t1", duration=1, priority=Priority.HIGH))
    assert s.submit(Task(id="t2", duration=2, priority=Priority.NORMAL))
    assert len(s.pending_tasks) == 2


def test_scheduler_fixed_schedule_conflict():
    # Reservations are bounded by capacity: with 2 workers one overlapping slot can be reserved.
    s = ElasticPriorityScheduler(initial_workers=1, max_workers=2)
    s.start()
    base = time.time() + 100
    assert s.submit(Task(id="f1", duration=10, fixed_start=base))
    assert not s.submit(Task(id="f2", duration=10, fixed_start=base + 5)), "overlapping reservation accepted"
    assert s.submit(Task(id="f3", duration=10, fixed_start=base + 30)), "non-overlapping reservation rejected"
    s.stop()


def test_quality_metrics_without_completions():
    s = ElasticPriorityScheduler()
    assert s.get_metrics().schedule_quality == 1.0, "Empty schedule should have 1.0 quality"
    s.submit(Task(id="p", duration=1))
    assert s.get_metrics().schedule_quality == 0.5, "Pending without completed should have 0.5 quality"


def test_idle_scheduler_does_not_scale_up():
    s = ElasticPriorityScheduler(initial_workers=1, max_workers=4)
    s.start()
    time.sleep(2.5)
    workers = s.get_metrics().worker_count
    s.stop()
    assert workers == 1


# ----------------------------------------------------------------------------- integration


def test_integration_mixed_workload():
    scheduler = ElasticPriorityScheduler(initial_workers=1, max_workers=4, scale_up_threshold=0.60, scale_down_threshold=0.95)
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
    for task in tasks:
        scheduler.submit(task)
    wait_for(scheduler, len(tasks), 15)
    scheduler.stop()
    metrics = scheduler.get_metrics()
    assert scheduler.completed_count == len(tasks), f"Not all tasks completed: {scheduler.completed_count}"
    assert metrics.schedule_quality >= 0.5, f"Quality too low: {metrics.schedule_quality}"
    assert scheduler.get_cost() > 0


def test_integration_fixed_schedule():
    scheduler = ElasticPriorityScheduler(initial_workers=2, max_workers=4)
    fixed_start = time.time() + 1.0
    tasks = [
        Task(id="fixed-task", duration=0.5, priority=Priority.NORMAL, fixed_start=fixed_start),
        Task(id="filler-1", duration=0.3, priority=Priority.LOW),
        Task(id="filler-2", duration=0.3, priority=Priority.LOW),
    ]
    scheduler.start()
    for task in tasks:
        scheduler.submit(task)
    wait_for(scheduler, len(tasks), 10)
    scheduler.stop()
    metrics = scheduler.get_metrics()
    assert scheduler.completed_count == len(tasks), "Not all tasks completed"
    assert metrics.fixed_schedule_hits >= 0.9, f"Fixed schedule not respected: {metrics.fixed_schedule_hits}"


def test_integration_elastic_scaling():
    scheduler = ElasticPriorityScheduler(initial_workers=1, max_workers=4, scale_up_threshold=0.50, scale_down_threshold=0.95)
    scheduler.start()
    initial_workers = scheduler.get_metrics().worker_count
    burst = [Task(id=f"burst-{i}", duration=1.0, priority=Priority.HIGH) for i in range(10)]
    for task in burst:
        scheduler.submit(task)
    max_seen = initial_workers
    for _ in range(50):
        time.sleep(0.1)
        max_seen = max(max_seen, scheduler.get_metrics().worker_count)
    wait_for(scheduler, len(burst), 20)
    scheduler.stop()
    assert scheduler.completed_count == len(burst), "Not all tasks completed"
    assert max_seen > initial_workers, "burst should trigger scale-up"


def test_integration_priority_ordering():
    # One worker, kept busy while the rest queue up, so the order is decided by priority alone.
    scheduler = ElasticPriorityScheduler(initial_workers=1, max_workers=1)
    tasks = [
        Task(id="blocker", duration=0.5, priority=Priority.NORMAL),
        Task(id="low-1", duration=0.1, priority=Priority.LOW),
        Task(id="low-2", duration=0.1, priority=Priority.LOW),
        Task(id="critical", duration=0.1, priority=Priority.CRITICAL),
    ]
    scheduler.start()
    for task in tasks:
        scheduler.submit(task)
    wait_for(scheduler, len(tasks), 10)
    scheduler.stop()
    assert scheduler.completed_count == len(tasks), "Not all tasks completed"
    assert start_order(scheduler) == ["blocker", "critical", "low-1", "low-2"]


def test_dependencies_wait_for_later_submissions():
    s = ElasticPriorityScheduler(initial_workers=2, max_workers=2)
    s.start()
    s.submit(Task(id="child", duration=0.05, dependencies=["parent"]))
    time.sleep(0.2)
    assert s.completed_count == 0
    s.submit(Task(id="parent", duration=0.05))
    wait_for(s, 2, 5)
    s.stop()
    assert start_order(s) == ["parent", "child"]
