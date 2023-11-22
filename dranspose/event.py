import zmq
from pydantic_core.core_schema import ValidationInfo

from dranspose.protocol import EventNumber, StreamName
from pydantic import BaseModel, ConfigDict, computed_field, field_validator


class StreamData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    typ: str
    frames: list[zmq.Frame]

    @computed_field
    @property
    def length(self) -> int:
        return len(self.frames)


class InternalWorkerMessage(BaseModel):
    event_number: EventNumber
    streams: dict[StreamName, StreamData] = {}

    def get_all_frames(self) -> list[zmq.Frame]:
        return [frame for stream in self.streams.values() for frame in stream.frames]


class EventData(BaseModel):
    event_number: EventNumber
    streams: dict[StreamName, StreamData]
