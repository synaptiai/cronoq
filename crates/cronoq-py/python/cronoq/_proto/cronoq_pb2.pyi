from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Priority(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRIORITY_UNSPECIFIED: _ClassVar[Priority]
    PRIORITY_CRITICAL: _ClassVar[Priority]
    PRIORITY_HIGH: _ClassVar[Priority]
    PRIORITY_NORMAL: _ClassVar[Priority]
    PRIORITY_LOW: _ClassVar[Priority]

class TaskStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_STATUS_UNSPECIFIED: _ClassVar[TaskStatus]
    TASK_STATUS_BLOCKED: _ClassVar[TaskStatus]
    TASK_STATUS_SCHEDULED: _ClassVar[TaskStatus]
    TASK_STATUS_READY: _ClassVar[TaskStatus]
    TASK_STATUS_RUNNING: _ClassVar[TaskStatus]
    TASK_STATUS_RETRYING: _ClassVar[TaskStatus]
    TASK_STATUS_SUCCEEDED: _ClassVar[TaskStatus]
    TASK_STATUS_FAILED: _ClassVar[TaskStatus]
    TASK_STATUS_DEAD_LETTERED: _ClassVar[TaskStatus]
    TASK_STATUS_CANCELLED: _ClassVar[TaskStatus]
    TASK_STATUS_SKIPPED: _ClassVar[TaskStatus]
    TASK_STATUS_MISSED: _ClassVar[TaskStatus]
    TASK_STATUS_SHED: _ClassVar[TaskStatus]

class EdgeCond(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EDGE_COND_ON_SUCCESS: _ClassVar[EdgeCond]
    EDGE_COND_ON_COMPLETE: _ClassVar[EdgeCond]
    EDGE_COND_ON_FAILURE: _ClassVar[EdgeCond]

class MissPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MISS_POLICY_RUN_LATE: _ClassVar[MissPolicy]
    MISS_POLICY_SKIP: _ClassVar[MissPolicy]
    MISS_POLICY_FAIL: _ClassVar[MissPolicy]
PRIORITY_UNSPECIFIED: Priority
PRIORITY_CRITICAL: Priority
PRIORITY_HIGH: Priority
PRIORITY_NORMAL: Priority
PRIORITY_LOW: Priority
TASK_STATUS_UNSPECIFIED: TaskStatus
TASK_STATUS_BLOCKED: TaskStatus
TASK_STATUS_SCHEDULED: TaskStatus
TASK_STATUS_READY: TaskStatus
TASK_STATUS_RUNNING: TaskStatus
TASK_STATUS_RETRYING: TaskStatus
TASK_STATUS_SUCCEEDED: TaskStatus
TASK_STATUS_FAILED: TaskStatus
TASK_STATUS_DEAD_LETTERED: TaskStatus
TASK_STATUS_CANCELLED: TaskStatus
TASK_STATUS_SKIPPED: TaskStatus
TASK_STATUS_MISSED: TaskStatus
TASK_STATUS_SHED: TaskStatus
EDGE_COND_ON_SUCCESS: EdgeCond
EDGE_COND_ON_COMPLETE: EdgeCond
EDGE_COND_ON_FAILURE: EdgeCond
MISS_POLICY_RUN_LATE: MissPolicy
MISS_POLICY_SKIP: MissPolicy
MISS_POLICY_FAIL: MissPolicy

class TaskRef(_message.Message):
    __slots__ = ("id", "key")
    ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    id: int
    key: str
    def __init__(self, id: _Optional[int] = ..., key: _Optional[str] = ...) -> None: ...

class Dep(_message.Message):
    __slots__ = ("target", "cond")
    TARGET_FIELD_NUMBER: _ClassVar[int]
    COND_FIELD_NUMBER: _ClassVar[int]
    target: TaskRef
    cond: EdgeCond
    def __init__(self, target: _Optional[_Union[TaskRef, _Mapping]] = ..., cond: _Optional[_Union[EdgeCond, str]] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ("max_attempts", "initial_backoff_s", "multiplier", "max_backoff_s", "jitter")
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_BACKOFF_S_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    MAX_BACKOFF_S_FIELD_NUMBER: _ClassVar[int]
    JITTER_FIELD_NUMBER: _ClassVar[int]
    max_attempts: int
    initial_backoff_s: float
    multiplier: float
    max_backoff_s: float
    jitter: bool
    def __init__(self, max_attempts: _Optional[int] = ..., initial_backoff_s: _Optional[float] = ..., multiplier: _Optional[float] = ..., max_backoff_s: _Optional[float] = ..., jitter: _Optional[bool] = ...) -> None: ...

class TaskSpec(_message.Message):
    __slots__ = ("payload", "content_type", "kind", "pool", "priority", "key", "deadline_in_s", "deadline_unix_ns", "start_in_s", "start_unix_ns", "tolerance_s", "on_miss", "duration_hint_s", "timeout_s", "retry", "deps", "resources", "tags")
    class ResourcesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    POOL_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_IN_S_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    START_IN_S_FIELD_NUMBER: _ClassVar[int]
    START_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    TOLERANCE_S_FIELD_NUMBER: _ClassVar[int]
    ON_MISS_FIELD_NUMBER: _ClassVar[int]
    DURATION_HINT_S_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_S_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    DEPS_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    content_type: str
    kind: str
    pool: str
    priority: Priority
    key: str
    deadline_in_s: float
    deadline_unix_ns: int
    start_in_s: float
    start_unix_ns: int
    tolerance_s: float
    on_miss: MissPolicy
    duration_hint_s: float
    timeout_s: float
    retry: RetryPolicy
    deps: _containers.RepeatedCompositeFieldContainer[Dep]
    resources: _containers.ScalarMap[str, int]
    tags: _containers.ScalarMap[str, str]
    def __init__(self, payload: _Optional[bytes] = ..., content_type: _Optional[str] = ..., kind: _Optional[str] = ..., pool: _Optional[str] = ..., priority: _Optional[_Union[Priority, str]] = ..., key: _Optional[str] = ..., deadline_in_s: _Optional[float] = ..., deadline_unix_ns: _Optional[int] = ..., start_in_s: _Optional[float] = ..., start_unix_ns: _Optional[int] = ..., tolerance_s: _Optional[float] = ..., on_miss: _Optional[_Union[MissPolicy, str]] = ..., duration_hint_s: _Optional[float] = ..., timeout_s: _Optional[float] = ..., retry: _Optional[_Union[RetryPolicy, _Mapping]] = ..., deps: _Optional[_Iterable[_Union[Dep, _Mapping]]] = ..., resources: _Optional[_Mapping[str, int]] = ..., tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class SubmitRequest(_message.Message):
    __slots__ = ("task", "wait")
    TASK_FIELD_NUMBER: _ClassVar[int]
    WAIT_FIELD_NUMBER: _ClassVar[int]
    task: TaskSpec
    wait: bool
    def __init__(self, task: _Optional[_Union[TaskSpec, _Mapping]] = ..., wait: _Optional[bool] = ...) -> None: ...

class SubmitResponse(_message.Message):
    __slots__ = ("id", "duplicate", "result")
    ID_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    id: int
    duplicate: bool
    result: TaskResult
    def __init__(self, id: _Optional[int] = ..., duplicate: _Optional[bool] = ..., result: _Optional[_Union[TaskResult, _Mapping]] = ...) -> None: ...

class SubmitBatchRequest(_message.Message):
    __slots__ = ("tasks",)
    TASKS_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[TaskSpec]
    def __init__(self, tasks: _Optional[_Iterable[_Union[TaskSpec, _Mapping]]] = ...) -> None: ...

class SubmitBatchResponse(_message.Message):
    __slots__ = ("tasks",)
    TASKS_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[SubmitResponse]
    def __init__(self, tasks: _Optional[_Iterable[_Union[SubmitResponse, _Mapping]]] = ...) -> None: ...

class CancelResponse(_message.Message):
    __slots__ = ("cancelled",)
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    cancelled: bool
    def __init__(self, cancelled: _Optional[bool] = ...) -> None: ...

class TaskInfo(_message.Message):
    __slots__ = ("id", "status", "found")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    id: int
    status: TaskStatus
    found: bool
    def __init__(self, id: _Optional[int] = ..., status: _Optional[_Union[TaskStatus, str]] = ..., found: _Optional[bool] = ...) -> None: ...

class TaskResult(_message.Message):
    __slots__ = ("id", "status", "output", "message")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    id: int
    status: TaskStatus
    output: bytes
    message: str
    def __init__(self, id: _Optional[int] = ..., status: _Optional[_Union[TaskStatus, str]] = ..., output: _Optional[bytes] = ..., message: _Optional[str] = ...) -> None: ...

class WaitRequest(_message.Message):
    __slots__ = ("task", "timeout_s")
    TASK_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_S_FIELD_NUMBER: _ClassVar[int]
    task: TaskRef
    timeout_s: float
    def __init__(self, task: _Optional[_Union[TaskRef, _Mapping]] = ..., timeout_s: _Optional[float] = ...) -> None: ...

class WatchRequest(_message.Message):
    __slots__ = ("pools",)
    POOLS_FIELD_NUMBER: _ClassVar[int]
    pools: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, pools: _Optional[_Iterable[str]] = ...) -> None: ...

class StatusEvent(_message.Message):
    __slots__ = ("id", "pool", "key", "status", "attempt", "at_unix_ns", "message")
    ID_FIELD_NUMBER: _ClassVar[int]
    POOL_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    AT_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    id: int
    pool: str
    key: str
    status: TaskStatus
    attempt: int
    at_unix_ns: int
    message: str
    def __init__(self, id: _Optional[int] = ..., pool: _Optional[str] = ..., key: _Optional[str] = ..., status: _Optional[_Union[TaskStatus, str]] = ..., attempt: _Optional[int] = ..., at_unix_ns: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...

class Register(_message.Message):
    __slots__ = ("pool", "slots", "worker_name")
    POOL_FIELD_NUMBER: _ClassVar[int]
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    WORKER_NAME_FIELD_NUMBER: _ClassVar[int]
    pool: str
    slots: int
    worker_name: str
    def __init__(self, pool: _Optional[str] = ..., slots: _Optional[int] = ..., worker_name: _Optional[str] = ...) -> None: ...

class Heartbeat(_message.Message):
    __slots__ = ("lease",)
    LEASE_FIELD_NUMBER: _ClassVar[int]
    lease: int
    def __init__(self, lease: _Optional[int] = ...) -> None: ...

class Complete(_message.Message):
    __slots__ = ("lease", "output")
    LEASE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    lease: int
    output: bytes
    def __init__(self, lease: _Optional[int] = ..., output: _Optional[bytes] = ...) -> None: ...

class Fail(_message.Message):
    __slots__ = ("lease", "message", "permanent", "retry_after_s")
    LEASE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    PERMANENT_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_S_FIELD_NUMBER: _ClassVar[int]
    lease: int
    message: str
    permanent: bool
    retry_after_s: float
    def __init__(self, lease: _Optional[int] = ..., message: _Optional[str] = ..., permanent: _Optional[bool] = ..., retry_after_s: _Optional[float] = ...) -> None: ...

class SetSlots(_message.Message):
    __slots__ = ("slots",)
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    slots: int
    def __init__(self, slots: _Optional[int] = ...) -> None: ...

class WorkerMessage(_message.Message):
    __slots__ = ("register", "heartbeat", "complete", "fail", "set_slots")
    REGISTER_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_FIELD_NUMBER: _ClassVar[int]
    FAIL_FIELD_NUMBER: _ClassVar[int]
    SET_SLOTS_FIELD_NUMBER: _ClassVar[int]
    register: Register
    heartbeat: Heartbeat
    complete: Complete
    fail: Fail
    set_slots: SetSlots
    def __init__(self, register: _Optional[_Union[Register, _Mapping]] = ..., heartbeat: _Optional[_Union[Heartbeat, _Mapping]] = ..., complete: _Optional[_Union[Complete, _Mapping]] = ..., fail: _Optional[_Union[Fail, _Mapping]] = ..., set_slots: _Optional[_Union[SetSlots, _Mapping]] = ...) -> None: ...

class Registered(_message.Message):
    __slots__ = ("worker_id", "lease_ttl_s")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_S_FIELD_NUMBER: _ClassVar[int]
    worker_id: int
    lease_ttl_s: float
    def __init__(self, worker_id: _Optional[int] = ..., lease_ttl_s: _Optional[float] = ...) -> None: ...

class Lease(_message.Message):
    __slots__ = ("lease", "task_id", "attempt", "payload", "content_type", "kind", "key", "tags", "timeout_s", "deadline_unix_ns")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    LEASE_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_S_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    lease: int
    task_id: int
    attempt: int
    payload: bytes
    content_type: str
    kind: str
    key: str
    tags: _containers.ScalarMap[str, str]
    timeout_s: float
    deadline_unix_ns: int
    def __init__(self, lease: _Optional[int] = ..., task_id: _Optional[int] = ..., attempt: _Optional[int] = ..., payload: _Optional[bytes] = ..., content_type: _Optional[str] = ..., kind: _Optional[str] = ..., key: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., timeout_s: _Optional[float] = ..., deadline_unix_ns: _Optional[int] = ...) -> None: ...

class Abort(_message.Message):
    __slots__ = ("lease", "reason")
    LEASE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    lease: int
    reason: str
    def __init__(self, lease: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class ServerMessage(_message.Message):
    __slots__ = ("registered", "lease", "abort")
    REGISTERED_FIELD_NUMBER: _ClassVar[int]
    LEASE_FIELD_NUMBER: _ClassVar[int]
    ABORT_FIELD_NUMBER: _ClassVar[int]
    registered: Registered
    lease: Lease
    abort: Abort
    def __init__(self, registered: _Optional[_Union[Registered, _Mapping]] = ..., lease: _Optional[_Union[Lease, _Mapping]] = ..., abort: _Optional[_Union[Abort, _Mapping]] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PoolSnapshot(_message.Message):
    __slots__ = ("name", "capacity", "busy", "held", "desired", "pending_by_class", "reservations", "dispatched", "succeeded", "failed", "priority_adherence", "fixed_start_hit_rate", "utilization", "schedule_efficiency", "wait_p50_s", "wait_p90_s", "wait_p99_s", "cost")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    BUSY_FIELD_NUMBER: _ClassVar[int]
    HELD_FIELD_NUMBER: _ClassVar[int]
    DESIRED_FIELD_NUMBER: _ClassVar[int]
    PENDING_BY_CLASS_FIELD_NUMBER: _ClassVar[int]
    RESERVATIONS_FIELD_NUMBER: _ClassVar[int]
    DISPATCHED_FIELD_NUMBER: _ClassVar[int]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_ADHERENCE_FIELD_NUMBER: _ClassVar[int]
    FIXED_START_HIT_RATE_FIELD_NUMBER: _ClassVar[int]
    UTILIZATION_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_EFFICIENCY_FIELD_NUMBER: _ClassVar[int]
    WAIT_P50_S_FIELD_NUMBER: _ClassVar[int]
    WAIT_P90_S_FIELD_NUMBER: _ClassVar[int]
    WAIT_P99_S_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    name: str
    capacity: int
    busy: int
    held: int
    desired: int
    pending_by_class: _containers.RepeatedScalarFieldContainer[int]
    reservations: int
    dispatched: int
    succeeded: int
    failed: int
    priority_adherence: float
    fixed_start_hit_rate: float
    utilization: float
    schedule_efficiency: float
    wait_p50_s: float
    wait_p90_s: float
    wait_p99_s: float
    cost: float
    def __init__(self, name: _Optional[str] = ..., capacity: _Optional[int] = ..., busy: _Optional[int] = ..., held: _Optional[int] = ..., desired: _Optional[int] = ..., pending_by_class: _Optional[_Iterable[int]] = ..., reservations: _Optional[int] = ..., dispatched: _Optional[int] = ..., succeeded: _Optional[int] = ..., failed: _Optional[int] = ..., priority_adherence: _Optional[float] = ..., fixed_start_hit_rate: _Optional[float] = ..., utilization: _Optional[float] = ..., schedule_efficiency: _Optional[float] = ..., wait_p50_s: _Optional[float] = ..., wait_p90_s: _Optional[float] = ..., wait_p99_s: _Optional[float] = ..., cost: _Optional[float] = ...) -> None: ...

class SnapshotResponse(_message.Message):
    __slots__ = ("pools", "live_tasks", "running", "pending")
    POOLS_FIELD_NUMBER: _ClassVar[int]
    LIVE_TASKS_FIELD_NUMBER: _ClassVar[int]
    RUNNING_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    pools: _containers.RepeatedCompositeFieldContainer[PoolSnapshot]
    live_tasks: int
    running: int
    pending: int
    def __init__(self, pools: _Optional[_Iterable[_Union[PoolSnapshot, _Mapping]]] = ..., live_tasks: _Optional[int] = ..., running: _Optional[int] = ..., pending: _Optional[int] = ...) -> None: ...

class RecurringRequest(_message.Message):
    __slots__ = ("name", "cron", "tz", "template", "allow_overlap")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CRON_FIELD_NUMBER: _ClassVar[int]
    TZ_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    ALLOW_OVERLAP_FIELD_NUMBER: _ClassVar[int]
    name: str
    cron: str
    tz: str
    template: TaskSpec
    allow_overlap: bool
    def __init__(self, name: _Optional[str] = ..., cron: _Optional[str] = ..., tz: _Optional[str] = ..., template: _Optional[_Union[TaskSpec, _Mapping]] = ..., allow_overlap: _Optional[bool] = ...) -> None: ...

class RemoveRecurringRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class RemoveRecurringResponse(_message.Message):
    __slots__ = ("removed",)
    REMOVED_FIELD_NUMBER: _ClassVar[int]
    removed: bool
    def __init__(self, removed: _Optional[bool] = ...) -> None: ...
