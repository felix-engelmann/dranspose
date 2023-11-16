import pickle
from dataclasses import dataclass, field
from typing import NewType, Literal

from pydantic import AnyUrl, UUID4, BaseModel, validate_call

import zmq
from serde import serde


class RedisKeys:
    PREFIX = "dranspose"

    @staticmethod
    @validate_call
    def config(typ: Literal["ingester", "worker"] = None, instance: str = None) -> str:
        return f"{RedisKeys.PREFIX}:{typ or '*'}:{instance or '*'}:config"

    @staticmethod
    @validate_call
    def ready(uuid: UUID4 = None) -> str:
        return f"{RedisKeys.PREFIX}:ready:{uuid}"

    @staticmethod
    @validate_call
    def assigned(uuid: UUID4 = None) -> str:
        return f"{RedisKeys.PREFIX}:assigned:{uuid}"

    @staticmethod
    def updates() -> str:
        return f"{RedisKeys.PREFIX}:controller:updates"

class ProtocolException(Exception):
    pass

Stream = NewType("Stream", str)

class ControllerUpdate(BaseModel):
    mapping_uuid: UUID4

class IngesterState(BaseModel):
    name: str
    url: AnyUrl
    mapping_uuid: UUID4 | None = None
    streams: list[Stream] = []


class WorkerState(BaseModel):
    name: str
    mapping_uuid: UUID4 | None = None
    ingesters: list[IngesterState] = []


class EnsembleState(BaseModel):
    ingesters: list[IngesterState]
    workers: list[WorkerState]

    def get_streams(self) -> list[Stream]:
        ingester_streams = set([s for i in self.ingesters for s in i.streams])
        print("ingester_streams", ingester_streams)
        print("w streams", [[i.streams for i in w.ingesters] for w in self.workers])
        worker_streams = [set([s for i in w.ingesters for s in i.streams]) for w in self.workers]

        return list(ingester_streams.intersection(*worker_streams))


PREFIX = RedisKeys.PREFIX # deprecated
