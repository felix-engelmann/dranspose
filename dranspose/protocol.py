import datetime
import time
import uuid
from enum import Enum
from typing import NewType, Literal, Annotated, Optional, TypeAlias, Any

from pydantic import UUID4, BaseModel, validate_call, UrlConstraints, Field, TypeAdapter

from functools import cache, cached_property

from pydantic_core import Url

ZmqUrl = Annotated[Url, UrlConstraints(allowed_schemes=["tcp"])]

StreamName = NewType("StreamName", str)
"""
strongly typed stream name (str)
"""
WorkerName = NewType("WorkerName", str)
_WorkerTagT: TypeAlias = str
WorkerTag = NewType("WorkerTag", _WorkerTagT)
"""
Strongly typed worker tag (str)
"""
IngesterName = NewType("IngesterName", str)
VirtualConstraint = NewType("VirtualConstraint", int)
"""
Strongly typed constraint for workers (int)
"""
EventNumber = NewType("EventNumber", int)
"""
strongly typed event number (int)
"""

ParameterName = NewType("ParameterName", str)

HashDigest = NewType("HashDigest", str)

GENERIC_WORKER = WorkerTag("generic")

MappingName = NewType("MappingName", str)


class VirtualWorker(BaseModel):
    """
    virtual worker with a number and tags

    Attributes:
        tags: set of tags which a worker must have to get this event
        constraint: a VirtualConstraint to which worker this event should be delivered, if None, deliver to all workers with matching tags
    """

    tags: set[WorkerTag] = {GENERIC_WORKER}
    constraint: Optional[VirtualConstraint] = None


class RedisKeys:
    PREFIX = "dranspose"

    @staticmethod
    @cache
    @validate_call
    def config(
        typ: Literal["ingester", "worker", "reducer", "*"] = "*",
        instance: IngesterName | WorkerName | Literal["reducer", "*"] = "*",
    ) -> str:
        if typ == "reducer":
            instance = "reducer"
        return f"{RedisKeys.PREFIX}:{typ}:{instance}:config"

    @staticmethod
    @cache
    @validate_call
    def ready(uuid: Optional[UUID4 | Literal["*"]] = None) -> str:
        return f"{RedisKeys.PREFIX}:ready:{uuid}"

    @staticmethod
    @cache
    @validate_call
    def assigned(uuid: Optional[UUID4 | Literal["*"]] = None) -> str:
        return f"{RedisKeys.PREFIX}:assigned:{uuid}"

    @staticmethod
    @cache
    @validate_call
    def clock(uuid: Optional[UUID4 | Literal["*"]] = None) -> str:
        return f"{RedisKeys.PREFIX}:clock:{uuid}"

    @staticmethod
    @cache
    @validate_call
    def lock() -> str:
        return f"{RedisKeys.PREFIX}:controller_lock"

    @staticmethod
    @cache
    def updates() -> str:
        return f"{RedisKeys.PREFIX}:controller:updates"

    @staticmethod
    @cache
    @validate_call
    def parameter_updates(channel: Literal["custom", "internal"]) -> str:
        return f"{RedisKeys.PREFIX}:parameter-{channel}:updates"


class ProtocolException(Exception):
    pass


class ControllerUpdate(BaseModel):
    mapping_uuid: UUID4
    parameters_version: dict[ParameterName, UUID4]
    target_parameters_hash: Optional[HashDigest] = None
    active_streams: list[StreamName] = []
    finished: bool = False


class ParameterUpdate(BaseModel):
    parameter_hash_target: HashDigest
    update: Any


class WorkAssignment(BaseModel):
    event_number: EventNumber
    assignments: dict[StreamName, list[WorkerName]]

    def get_workers_for_streams(self, streams: list[StreamName]) -> "WorkAssignment":
        ret = WorkAssignment(event_number=self.event_number, assignments={})
        for stream in streams:
            if stream in self.assignments:
                ret.assignments[stream] = self.assignments[stream]
        return ret

    def get_all_workers(self) -> set[WorkerName]:
        return set([x for stream in self.assignments.values() for x in stream])


WorkAssignmentList = TypeAdapter(list[WorkAssignment])


class DistributedStateEnum(Enum):
    READY = "ready"
    IDLE = "idle"
    FINISHED = "finished"


class WorkerTimes(BaseModel):
    get_assignments: float = 0.0
    get_messages: float = 0.0
    assemble_event: float = 0.0
    custom_code: float = 0.0
    send_result: float = 0.0
    no_events: int = 1

    @classmethod
    def from_timestamps(
        cls,
        start: float,
        assignments: float,
        messages: float,
        event: float,
        custom: float,
        send: float,
    ) -> "WorkerTimes":
        return WorkerTimes(
            get_assignments=assignments - start,
            get_messages=messages - assignments,
            assemble_event=event - messages,
            custom_code=custom - event,
            send_result=send - custom,
        )

    def __add__(self, other: "WorkerTimes") -> "WorkerTimes":
        return WorkerTimes(
            get_assignments=self.get_assignments + other.get_assignments,
            get_messages=self.get_messages + other.get_messages,
            assemble_event=self.assemble_event + other.assemble_event,
            custom_code=self.custom_code + other.custom_code,
            send_result=self.send_result + other.send_result,
            no_events=self.no_events + other.no_events,
        )

    @cached_property
    def total(self) -> float:
        return self.get_assignments + self.get_messages + self.active

    @cached_property
    def active(self) -> float:
        return self.assemble_event + self.custom_code + self.send_result

    @cached_property
    def load(self) -> float:
        return self.active / self.total


class IntervalLoad(BaseModel):
    total: float
    active: float
    events: int

    @cached_property
    def load(self) -> float:
        return self.active / self.total


class WorkerLoad(BaseModel):
    last_event: int
    intervals: dict[int | Literal["scan"], IntervalLoad]


SystemLoadType = dict[WorkerName, WorkerLoad]


class BaseUpdate(BaseModel):
    state: DistributedStateEnum


class WorkerUpdate(BaseUpdate):
    source: Literal["worker"] = "worker"
    completed: list[EventNumber] = []
    worker: WorkerName
    has_result: list[bool] = []
    processing_times: Optional[WorkerTimes] = None


class ReducerUpdate(BaseUpdate):
    source: Literal["reducer"] = "reducer"
    completed: Optional[EventNumber] = None
    worker: Optional[WorkerName] = None


class IngesterUpdate(BaseUpdate):
    source: Literal["ingester"] = "ingester"
    ingester: IngesterName


DistributedUpdateType = WorkerUpdate | ReducerUpdate | IngesterUpdate

DistributedUpdate = TypeAdapter(WorkerUpdate | ReducerUpdate | IngesterUpdate)


class BuildGitMeta(BaseModel):
    commit_hash: HashDigest
    branch_name: str
    timestamp: datetime.datetime
    repository_url: Url


class ParameterUpdateResponse(BaseModel):
    updated_keys: list[str]
    target_hash: HashDigest


class ParameterState(BaseModel):
    parameter_signature: Optional[str] = None
    parameter_hash: Optional[HashDigest] = None


class DistributedState(BaseModel):
    service_uuid: UUID4 = Field(default_factory=uuid.uuid4)
    mapping_uuid: Optional[UUID4] = None
    dranspose_version: Optional[str] = None
    mapreduce_version: Optional[BuildGitMeta] = None
    parameters: dict[str, ParameterState] = {}
    processed_events: int = 0
    event_rate: float = 0.0

    # write a h5 dump function for metadata


class ConnectedWorker(BaseModel):
    name: WorkerName
    service_uuid: UUID4
    last_seen: float = Field(default_factory=time.time)


class IngesterState(DistributedState):
    name: IngesterName
    url: ZmqUrl
    connected_workers: dict[UUID4, ConnectedWorker] = {}
    streams: list[StreamName] = []


class WorkerState(DistributedState):
    name: WorkerName
    ingesters: list[IngesterState] = []
    tags: set[WorkerTag] = {GENERIC_WORKER}


class ReducerState(DistributedState):
    name: str = "reducer"
    url: ZmqUrl


class EnsembleState(BaseModel):
    ingesters: list[IngesterState]
    workers: list[WorkerState]
    reducer: Optional[ReducerState]
    controller_version: Optional[str] = None

    def get_streams(self) -> list[StreamName]:
        ingester_streams = set([s for i in self.ingesters for s in i.streams])
        worker_streams = [
            set([s for i in w.ingesters for s in i.streams]) for w in self.workers
        ]

        return list(ingester_streams.intersection(*worker_streams))

    def get_workers(self) -> list[WorkerName]:
        return [w.name for w in self.workers]
