from typing import Literal, Optional

from pydantic import BaseModel, TypeAdapter, ConfigDict


class XspressStart(BaseModel):
    model_config = ConfigDict(extra="allow")

    htype: Literal["header"]
    filename: str


class XspressImage(BaseModel):
    model_config = ConfigDict(extra="allow")

    htype: Literal["image"]
    frame: int
    shape: list[int]
    exptime: Optional[float] = 1
    type: str
    compression: Optional[str] = "none"


class XspressEnd(BaseModel):
    model_config = ConfigDict(extra="allow")

    htype: Literal["series_end"]


XspressPacket = TypeAdapter(XspressStart | XspressImage | XspressEnd)
