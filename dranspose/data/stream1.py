from typing import Literal

from pydantic import BaseModel, TypeAdapter


class Stream1(BaseModel):
    msg_number: int


class Stream1Start(Stream1):
    htype: Literal["header"]
    filename: str


class Stream1Data(Stream1):
    htype: Literal["image"]
    frame: int
    shape: list[int]
    type: str
    compression: str


class Stream1End(Stream1):
    htype: Literal["series_end"]


Stream1Packet = TypeAdapter(Stream1Start | Stream1Data | Stream1End)
