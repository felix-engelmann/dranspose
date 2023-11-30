from typing import Any, Optional

import zmq
from pydantic_core.core_schema import ValidationInfo

from dranspose.protocol import EventNumber, StreamName, WorkerName
from pydantic import BaseModel, ConfigDict, computed_field, field_validator, UUID4


class StreamData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    typ: str
    frames: list[zmq.Frame | bytes]

    @computed_field
    @property
    def length(self) -> int:
        return len(self.frames)


    def get_bytes(self):
        return StreamData(typ=self.typ, frames=[frame.bytes if isinstance(frame, zmq.Frame) else frame for frame in self.frames])


class InternalWorkerMessage(BaseModel):
    event_number: EventNumber
    streams: dict[StreamName, StreamData] = {}

    def get_all_frames(self) -> list[zmq.Frame]:
        return [frame for stream in self.streams.values() for frame in stream.frames]


class ResultData(BaseModel):
    event_number: EventNumber
    worker: WorkerName
    parameters_uuid: Optional[UUID4]
    payload: Any


class EventData(BaseModel):
    event_number: EventNumber
    streams: dict[StreamName, StreamData]

    @classmethod
    def from_internals(cls, msgs: list[InternalWorkerMessage]) -> "EventData":
        assert len(msgs) > 0, "merge at least one message"
        assert (
            len(set([m.event_number for m in msgs])) == 1
        ), "Cannot merge data from different events"
        all_stream_names = [stream for m in msgs for stream in m.streams.keys()]
        assert len(all_stream_names) == len(
            set(all_stream_names)
        ), "Cannot merge data with duplicate streams"

        ret = EventData(event_number=msgs[0].event_number, streams={})
        for msg in msgs:
            ret.streams.update(msg.streams)
        return ret
