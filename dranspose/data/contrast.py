from typing import Literal

from pydantic import BaseModel, TypeAdapter


class ContrastStarted(BaseModel):
    status: Literal["started"]
    path: str
    scannr: int
    description: str


class ContrastRunning(BaseModel):
    status: Literal["running"]
    dt: float


class ContrastFinished(BaseModel):
    status: Literal["finished"]
    path: str
    scannr: int
    description: str


class ContrastHeartbeat(BaseModel):
    status: Literal["heartbeat"]


ContrastPacket = TypeAdapter(
    ContrastStarted | ContrastRunning | ContrastFinished | ContrastHeartbeat
)
