from datetime import datetime

from pydantic import BaseModel


class PositionCapField(BaseModel):
    name: str
    type: str
    value: int | float


class PositionCapStart(BaseModel):
    arm_time: datetime


class PositionCapValues(BaseModel):
    fields: dict[str, PositionCapField]


class PositionCapEnd(BaseModel):
    pass
