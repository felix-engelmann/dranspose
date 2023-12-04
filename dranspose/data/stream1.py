from typing import Literal

from pydantic import BaseModel, TypeAdapter, ConfigDict


class Stream1(BaseModel):
    msg_number: int


class Stream1Start(Stream1):
    model_config = ConfigDict(extra="allow")

    htype: Literal["header"]
    filename: str


class Stream1Data(Stream1):
    model_config = ConfigDict(extra="allow")

    htype: Literal["image"]
    frame: int
    shape: list[int]
    type: str
    compression: str


class Stream1End(Stream1):
    model_config = ConfigDict(extra="allow")

    htype: Literal["series_end"]


Stream1Packet = TypeAdapter(Stream1Start | Stream1Data | Stream1End)
