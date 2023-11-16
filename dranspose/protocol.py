import pickle
from dataclasses import dataclass, field
from enum import Enum
from typing import NewType, Literal, Annotated

from pydantic import AnyUrl, UUID4, BaseModel, validate_call, UrlConstraints

import zmq
from functools import cache

from pydantic_core import Url

ZmqUrl = Annotated[
    Url, UrlConstraints(allowed_schemes=["tcp"])
]

class RedisKeys:
    PREFIX = "dranspose"

    @staticmethod
    @cache
    @validate_call
    def config(typ: Literal["ingester", "worker", "*"] = "*", instance = "*") -> str:
        return f"{RedisKeys.PREFIX}:{typ}:{instance}:config"

    @staticmethod
    @cache
    @validate_call
    def ready(uuid: UUID4 | Literal["*"] | None = None) -> str:
        return f"{RedisKeys.PREFIX}:ready:{uuid}"

    @staticmethod
    @cache
    @validate_call
    def assigned(uuid: UUID4 | Literal["*"] | None = None) -> str:
        return f"{RedisKeys.PREFIX}:assigned:{uuid}"

    @staticmethod
    @cache
    def updates() -> str:
        return f"{RedisKeys.PREFIX}:controller:updates"


class ProtocolException(Exception):
    pass


StreamName = NewType("Stream", str)
WorkerName = NewType("WorkerName", str)
IngesterName = NewType("WorkerName", str)


class ControllerUpdate(BaseModel):
    mapping_uuid: UUID4


class WorkAssignment(BaseModel):
    event_number: int
    assignments: dict[StreamName, list[WorkerName]]

    def get_workers_for_streams(self, streams: list[StreamName]) -> "WorkAssignment":
        ret = WorkAssignment(event_number=self.event_number, assignments={})
        for stream in streams:
            if stream in self.assignments:
                ret.assignments[stream] = self.assignments[stream]
        return ret

    def get_all_workers(self) -> set[WorkerName]:
        return set([x for stream in self.assignments.values() for x in stream])


class WorkerStateEnum(Enum):
    IDLE = "idle"


class WorkerUpdate(BaseModel):
    state: WorkerStateEnum
    completed: int
    worker: WorkerName
    new: bool = False


class IngesterState(BaseModel):
    name: IngesterName
    url: ZmqUrl
    mapping_uuid: UUID4 | None = None
    streams: list[StreamName] = []


class WorkerState(BaseModel):
    name: WorkerName
    mapping_uuid: UUID4 | None = None
    ingesters: list[IngesterState] = []


class EnsembleState(BaseModel):
    ingesters: list[IngesterState]
    workers: list[WorkerState]

    def get_streams(self) -> list[StreamName]:
        ingester_streams = set([s for i in self.ingesters for s in i.streams])
        worker_streams = [
            set([s for i in w.ingesters for s in i.streams]) for w in self.workers
        ]

        return list(ingester_streams.intersection(*worker_streams))
