from typing import Literal

from pydantic import BaseModel, TypeAdapter, ConfigDict


class AlbaemBase(BaseModel):
    message_id: int
    version: Literal[1]


class AlbaemStart(AlbaemBase):
    """
    Example:
        ``` py
        AlbaemStart(
            message_id=1,
            version=1,
            message_type='series-start'
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    message_type: Literal["series-start"]


class AlbaemData(AlbaemBase):
    """
    While the original stream sends 3 separate zmq frames (no multipart), this returns a single packet.

    Example:
        ``` py
        AlbaemImage(
            message_id=2,
            version=1,
            message_type='data',
            frame_number=0,
            timestamp=7.72645192,
            acquisition_timestamp=1704807770443944912,
            channel1=-2.8302592615927417e-11,
            channel2=-6.091210149949596e-11,
            channel3=2.5349278603830644e-10,
            channel4=4.80528800718246e-10
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    message_type: Literal["data"]
    frame_number: int
    timestamp: float
    acquisition_timestamp: int


class AlbaemEnd(AlbaemBase):
    """
    Example:
        ``` py
        AlbaemEnd(
            message_id=6,
            version=1,
            message_type='series-end',
            detector_specific={'read_overflow': False, 'memory_overflow': False})
        ```
    """

    model_config = ConfigDict(extra="allow")

    message_type: Literal["series-end"]


AlbaemPacket = TypeAdapter(AlbaemStart | AlbaemData | AlbaemEnd)
"""
Union type for Albaem packets
"""
