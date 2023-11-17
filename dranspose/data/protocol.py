from typing import Literal

from pydantic import BaseModel, TypeAdapter


class Stream1(BaseModel):
    msg_number: int


class SeriesStart(Stream1):
    htype: Literal["header"]
    filename: str


class SeriesData(Stream1):
    htype: Literal["image"]
    frame: int
    shape: list[int]
    type: str
    compression: str


class SeriesEnd(Stream1):
    htype: Literal["series_end"]


Stream1Packet = TypeAdapter(SeriesStart | SeriesData | SeriesEnd)