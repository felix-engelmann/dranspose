from __future__ import annotations
from datetime import datetime
from typing import Optional

import zmq
from pydantic import BaseModel, TypeAdapter

from dranspose.event import StreamData


class PositionCapField(BaseModel):
    name: str
    type: str
    value: Optional[int | float] = None

    def parse(self, value: str) -> PositionCapField:
        ret = PositionCapField(type=self.type, name=self.name)
        if self.type == "uint32":
            ret.value = int(value)
        elif self.type == "double":
            ret.value = float(value)
        else:
            raise Exception("unknown data")
        return ret


class PositionCapStart(BaseModel):
    arm_time: datetime

    def to_stream_data(self, fields) -> StreamData:
        data = f"""arm_time: {self.arm_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
missed: 0
process: Scaled
format: ASCII
fields:
"""
        for field in fields:
            where = field.name.split(".")[-1]
            data += f" {field.name[:-len(where)-1]} {field.type} {where}\n"
        return StreamData(typ="PCAP", frames=[zmq.Frame(data.encode())])


class PositionCapValues(BaseModel):
    fields: dict[str, PositionCapField] = {}

    def to_stream_data(self) -> StreamData:
        data = " " + " ".join(map(lambda f: str(f.value), self.fields.values())) + "\n"
        return StreamData(typ="PCAP", frames=[zmq.Frame(data.encode())])


class PositionCapEnd(BaseModel):
    def to_stream_data(self) -> StreamData:
        data = "END 0 Disarmed\n"
        return StreamData(typ="PCAP", frames=[zmq.Frame(data.encode())])


PositionCapPacketType = PositionCapStart | PositionCapValues | PositionCapEnd

PositionCapPacket = TypeAdapter(PositionCapStart | PositionCapValues | PositionCapEnd)
"""
A union type for PCAP packets
"""
