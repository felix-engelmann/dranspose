from typing import Literal

import zmq
from pydantic import BaseModel, TypeAdapter, ConfigDict

from dranspose.event import StreamData


class Stream1(BaseModel):
    msg_number: int

    def to_stream_data(self):
        js = self.model_dump_json(exclude={"data"}).encode()
        frames = [zmq.Frame(js)]
        if hasattr(self, "data"):
            frames.append(zmq.Frame(self.data.tobytes()))
        return StreamData(typ="STINS", frames=frames)


class Stream1Start(Stream1):
    """
    Example:
        ``` py
        Stream1Start(
            msg_number=374,
            htype='header',
            filename='/data/visitors/....test.h5'
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["header"] = "header"
    filename: str


class Stream1Data(Stream1):
    """
    Example:
        ``` py
        Stream1Data(
            msg_number=375,
            htype='image',
            frame=0,
            shape=[831, 1475],
            type='float32',
            compression='none',
            data=array([[0., 0., 0., ..., 0., 0., 0.],
                [0., 0., 0., ..., 0., 0., 0.],
                [0., 0., 0., ..., 0., 0., 0.],
                ...,
                [0., 0., 0., ..., 0., 0., 0.],
                [0., 0., 0., ..., 0., 0., 0.],
                [0., 0., 0., ..., 0., 0., 0.]], dtype=float32)
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["image"] = "image"
    frame: int
    shape: list[int]
    type: str
    compression: str


class Stream1End(Stream1):
    """
    Example:
        ``` py
        Stream1End(
            msg_number=376,
            htype='series_end'
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["series_end"] = "series_end"


Stream1Packet = TypeAdapter(Stream1Start | Stream1Data | Stream1End)
"""
A union type for STINS packets
"""
