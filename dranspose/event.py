from typing import Any, Optional

import zmq
from pydantic_core.core_schema import ValidationInfo

from dranspose.protocol import EventNumber, StreamName, WorkerName
from pydantic import BaseModel, ConfigDict, computed_field, field_validator, UUID4


class StreamData(BaseModel):
    """
    Data container for a single stream with all zmq frames belonging to it

    Attributes:
         typ: arbitrary typ set by the ingester to common parsing
         frames: all frames received for this event for the stream
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    typ: str
    frames: list[zmq.Frame] | list[bytes]

    @computed_field  # type: ignore[misc]
    @property
    def length(self) -> int:
        """
        Calculates the length of frames.

        Returns:
             length of frames
        """
        return len(self.frames)

    def get_bytes(self) -> "StreamData":
        """
        Copies the data from the zmq buffer

        Returns:
             An object with a list of bytes.
        """
        return StreamData(
            typ=self.typ,
            frames=[
                frame.bytes if isinstance(frame, zmq.Frame) else frame
                for frame in self.frames
            ],
        )


class InternalWorkerMessage(BaseModel):
    """
    A container for partial events which carries one or more streams. This is the message between ingesters and workers.

    Attributes:
        event_number: event number
        streams: one or more streams from an ingester
    """

    event_number: EventNumber
    streams: dict[StreamName, StreamData] = {}

    def get_all_frames(self) -> list[zmq.Frame | bytes]:
        return [frame for stream in self.streams.values() for frame in stream.frames]


class ResultData(BaseModel):
    """
    Container for transferring results from the worker to the reducer. In enhances the pure payload with useful meta data

    Attributes:
        event_number: which event the result belongs to. NB: results may arrive out of order
        worker: which worker processed it.
        parameters_uuid: which version of parameters was used to process the event
        payload: the data return from the custom worker function: process_event
    """

    event_number: EventNumber
    worker: WorkerName
    parameters_uuid: Optional[UUID4]
    payload: Any


class EventData(BaseModel):
    """
    Main container for an event provided to the worker function

    Attributes:
        event_number:    Current event number relative to the trigger map provided
        streams:         Data for each stream present in the event
    """

    event_number: EventNumber
    streams: dict[StreamName, StreamData]

    @classmethod
    def from_internals(cls, msgs: list[InternalWorkerMessage]) -> "EventData":
        """
        Helper function to assemble an event from the internal messages received from the ingesters.
        This is a factory method

        Args:
             msgs: Internal messages each containing a subset of the streams for the event

        Returns:
            A new object with all data combined.
        """
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
