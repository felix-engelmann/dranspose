from typing import Literal

from pydantic import BaseModel, TypeAdapter



class XspressStart(BaseModel):
    htype: Literal["header"]
    filename: str


class XspressImage(BaseModel):
    htype: Literal["image"]
    frame: int
    shape: list[int]
    exptime: float
    type: str
    compression: str


class XspressEnd(BaseModel):
    htype: Literal["series_end"]


XspressPacket = TypeAdapter(XspressStart | XspressImage | XspressEnd)
