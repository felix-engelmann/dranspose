from typing import Literal

from pydantic import BaseModel, TypeAdapter, ConfigDict


class Stream1(BaseModel):
    msg_number: int


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

    htype: Literal["header"]
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

    htype: Literal["image"]
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

    htype: Literal["series_end"]


Stream1Packet = TypeAdapter(Stream1Start | Stream1Data | Stream1End)
"""
A union type for STINS packets
"""
