from typing import Literal

from pydantic import BaseModel, TypeAdapter, ConfigDict


class ContrastStarted(BaseModel):
    model_config = ConfigDict(extra='allow')

    status: Literal["started"]
    path: str
    scannr: int
    description: str


class ContrastRunning(BaseModel):
    model_config = ConfigDict(extra='allow')

    status: Literal["running"]
    dt: float


class ContrastFinished(BaseModel):
    model_config = ConfigDict(extra='allow')

    status: Literal["finished"]
    path: str
    scannr: int
    description: str


class ContrastHeartbeat(BaseModel):
    status: Literal["heartbeat"]


ContrastPacket = TypeAdapter(
    ContrastStarted | ContrastRunning | ContrastFinished | ContrastHeartbeat
)
