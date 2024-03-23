from __future__ import annotations
from datetime import datetime
from typing import Optional


from pydantic import BaseModel, TypeAdapter


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


class PositionCapValues(BaseModel):
    fields: dict[str, PositionCapField] = {}


class PositionCapEnd(BaseModel):
    pass


PositionCapPacketType = PositionCapStart | PositionCapValues | PositionCapEnd

PositionCapPacket = TypeAdapter(PositionCapStart | PositionCapValues | PositionCapEnd)
"""
A union type for PCAP packets
"""
