from typing import List, Literal

import zmq
from pydantic import BaseModel, TypeAdapter, ConfigDict

from dranspose.event import StreamData


class LecroyBase(BaseModel):
    frame: int
    htype: Literal["msg", "traces"]

    def to_stream_data(self) -> StreamData:
        dat = self.model_dump_json()
        return StreamData(typ="Lecroy", frames=[zmq.Frame(dat)])


class LecroyMessage(LecroyBase):
    # len(parts) parts[0]
    # 1 b'{"htype": "msg", "what": 0, "frame": 0}'
    """
    Example:
        ``` py
        LecroyMessage(
            htype='msg'
            what=0,
            frame=0,
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["msg"]
    what: Literal[0, 1, 2, 3]  # PREPARE = 0 START = 1 SEQEND = 2 STOP = 3


class LecroyStart(LecroyMessage):
    # len(parts) parts[0]
    # 1 b'{"htype": "msg", "what": 1, "frame": 0, "ntriggers": -1, "seqno": 0, "channels": [2, 4]}'
    """
    Example:
        ``` py
        LecroyStart(
            htype='msg'
            what=1,
            frame=0,
            ntriggers=-1
            seqno=0
            channels=[2, 4]
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    what: Literal[1]  # START = 1


class LecroySequence(LecroyMessage):
    # len(parts) parts[0]
    # 1 b'{"htype": "msg", "what": 2, "frame": 2}'
    """
    Example:
        ``` py
        LecroySequence(
            htype='msg'
            what=2,
            frame=2,
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    what: Literal[2]  # SEQEND = 2


class LecroyEnd(LecroyMessage):
    # len(parts) parts[0]
    """
    Example:
        ``` py
        LecroyEnd(
            htype='msg'
            what=3,
            frame=66,
            frames=66,
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    what: Literal[3]  # STOP = 3


class LecroyData(LecroyBase):
    # len(parts) parts[0]
    # 3 b'{"htype": "traces", "ch": 2, "ts": 1740563614.969933, "frame": 0, "shape": [1, 8002], "horiz_offset": -1.0000505879544622e-07, "horiz_interval": 1.25000001668929e-11, "dtype": "float64"}'
    """
    While the original stream sends 3 separate zmq frames (no multipart), this returns a single packet.

    Example:
        ``` py
        LecroyData(
            htype='traces',
            ch: 2,
            ts: 1740563614.969933,
            frame: 0,
            shape: [1, 8002],
            horiz_offset: -1.0000505879544622e-07,
            horiz_interval: 1.25000001668929e-11,
            dtype: "float64"
            )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["traces"]
    ch: int
    ts: float
    frame: int
    shape: List[int]
    dtype: str


LecroyPacket: TypeAdapter = TypeAdapter(LecroyStart | LecroyEnd | LecroySequence | LecroyData)  # type: ignore [type-arg]
"""
Union type for Lecroy packets
"""
