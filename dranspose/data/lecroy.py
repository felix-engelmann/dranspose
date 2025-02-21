from typing import Literal
from datetime import datetime

import zmq
from pydantic import BaseModel, TypeAdapter, ConfigDict

from dranspose.event import StreamData


class LecroyBase(BaseModel):
    message_id: int
    version: Literal[1]

    def to_stream_data(self) -> StreamData:
        dat = self.model_dump_json()
        return StreamData(typ="Lecroy", frames=[zmq.Frame(dat)])


class LecroyStart(LecroyBase):
    """
    Example:
        ``` py
        LecroyStart(
            message_id=1,
            version=1,
            message_type='series-start'
            arm_time="2024-10-07T08:49:10.627Z"
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    message_type: Literal["series-start"]
    arm_time: datetime


class LecroyData(LecroyBase):
    """
    While the original stream sends 3 separate zmq frames (no multipart), this returns a single packet.

    Example:
        ``` py
        LecroyImage(
            message_id=2,
            version=1,
            message_type="data",
            frame_number=0,
            inttime=[7.1160000000000005, 7.315, 7.093800000000001, 7.1584, 7.1322, 7.1062, 7.0206, 6.9082, 7.0164, 6.912, 6.966, 6.885000000000001, 6.7946, 6.731800000000001, 6.9636000000000005],
            triggernumber=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
            repeatindex=[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            energy=[8879.101048716273, 8879.299376689953, 8879.5003861779, 8879.699330779304, 8879.8999612314, 8880.099636263549, 8880.300291637586, 8880.499422033765, 8880.700264322526, 8880.900116547104, 8881.099615570443, 8881.29966467182, 8881.499998985424, 8881.700179495409, 8881.900461311128],
            INENC3.VAL.Mean=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        ```
    """

    model_config = ConfigDict(extra="allow")

    message_type: Literal["data"]
    frame_number: int


class LecroyEnd(LecroyBase):
    """
    Example:
        ``` py
        LecroyEnd(
            message_id=6,
            version=1,
            message_type='series-end')
        ```
    """

    model_config = ConfigDict(extra="allow")

    message_type: Literal["series-end"]


LecroyPacket = TypeAdapter(LecroyStart | LecroyData | LecroyEnd)
"""
Union type for Lecroy packets
"""
