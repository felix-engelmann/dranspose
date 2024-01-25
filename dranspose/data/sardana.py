from typing import Any

from pydantic import BaseModel, TypeAdapter


class SardanaDataDescription(BaseModel):
    description: dict[str, Any]


class SardanaRecordData(BaseModel):
    record: dict[str, Any]


class SardanaRecordEnd(BaseModel):
    end: dict[str, Any]


SardanaPacketType = SardanaDataDescription | SardanaRecordData | SardanaRecordEnd

SardanaPacket = TypeAdapter(
    SardanaDataDescription | SardanaRecordData | SardanaRecordEnd
)
