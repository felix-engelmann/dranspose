import zmq

from dranspose.protocol import EventNumber, StreamName
from pydantic import BaseModel, ConfigDict


class StreamData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    typ: str
    frames: list[zmq.Frame]


class EventData(BaseModel):
    event_number: EventNumber
    streams: dict[StreamName, StreamData]