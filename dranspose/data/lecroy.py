from enum import IntEnum
from typing import Any, List, Literal

from numpy.typing import NDArray
import zmq
from pydantic import BaseModel, TypeAdapter, ConfigDict

from dranspose.event import StreamData

LECROY_TYPE = "lecroy_9fwdWp6Rq7"


class WhatEnum(IntEnum):
    PREPARE = 0
    START = 1
    SEQEND = 2
    STOP = 3


class LecroyBase(BaseModel):
    frame: int

    def to_stream_data(self) -> StreamData:
        dat = self.model_dump_json()
        return StreamData(typ=LECROY_TYPE, frames=[zmq.Frame(dat)])


class LecroyPrepare(LecroyBase):
    # len(parts) parts[0]
    # 1 b'{"htype": "msg", "what": 0, "frame": 0}'
    """
    Example:
        ``` py
        LecroyStart(
            htype='msg'
            what=0,
            frame=0,
        )
        ```
    """

    htype: Literal["msg"]
    what: Literal[WhatEnum.PREPARE]  # PREPARE = 0


class LecroySeqStart(LecroyBase):
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

    htype: Literal["msg"]
    what: Literal[WhatEnum.START]  # START = 1
    ntriggers: int
    seqno: int
    channels: List[int]


class LecroySeqEnd(LecroyBase):
    # len(parts) parts[0]
    # 1 b'{"htype": "msg", "what": 2, "frame": 2}'
    """
    Example:
        ``` py
        LecroySeqEnd(
            htype='msg'
            what=2,
            frame=2,
        )
        ```
    """

    htype: Literal["msg"]
    what: Literal[WhatEnum.SEQEND]  # SEQEND = 2


class LecroyEnd(LecroyBase):
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

    frames: int
    what: Literal[WhatEnum.STOP]  # STOP = 3


class LecroyData(LecroyBase):
    # len(parts) parts[0]
    # 3 b'{"htype": "traces", "ch": 2, "ts": 1740563614.969933, "frame": 0, "shape": [1, 8002], "horiz_offset": -1.0000505879544622e-07, "horiz_interval": 1.25000001668929e-11, "dtype": "float64"}'
    """
    Each "traces" message has 3 zmq parts:
    metadata (json), waveforms (np.array), timestamps (list)

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

    htype: Literal["traces"]
    ch: int
    ts: float
    frame: int
    shape: List[int]
    dtype: str
    horiz_offset: float
    horiz_interval: float


class LecroyParsed(LecroySeqStart):
    # tells Pydantic to allow arbitrary Python types (like NumPy arrays)
    # without trying to validate them strictly
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    meta: List[LecroyData] = []
    data: List[NDArray[Any]] = []
    timestamps: List[List[float]] = []


LecroyPacket: TypeAdapter = TypeAdapter(LecroyPrepare | LecroySeqStart | LecroyEnd | LecroySeqEnd | LecroyData | LecroyParsed)  # type: ignore [type-arg]
"""
Union type for Lecroy packets
"""
