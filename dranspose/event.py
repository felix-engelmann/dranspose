from datetime import datetime, timezone
from typing import Any, Optional

import zmq
from cbor2 import CBORTag, CBOREncoder, CBORDecoder

from dranspose.protocol import EventNumber, StreamName, WorkerName, HashDigest
from pydantic import BaseModel, ConfigDict, computed_field, Field


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

    @computed_field  # type: ignore[prop-decorator]
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
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def get_all_frames(self) -> list[zmq.Frame | bytes]:
        return [frame for stream in self.streams.values() for frame in stream.frames]


def message_encoder(encoder: CBOREncoder, value: Any) -> None:
    # Tag number 4000 was chosen arbitrarily
    if isinstance(value, InternalWorkerMessage):
        encoder.encode(
            CBORTag(42877, (value.event_number, value.streams, value.created_at))
        )
    elif isinstance(value, StreamData):
        encoder.encode(CBORTag(42878, (value.typ, value.frames)))
    else:
        raise TypeError(type(value).__name__)


def message_tag_hook(
    decoder: CBORDecoder, tag: CBORTag, shareable_index: Any = None
) -> Any:
    if tag.tag == 42877:
        if len(tag.value) == 3:
            return InternalWorkerMessage(
                event_number=tag.value[0], streams=tag.value[1], created_at=tag.value[2]
            )
        return InternalWorkerMessage(event_number=tag.value[0], streams=tag.value[1])
    elif tag.tag == 42878:
        return StreamData(typ=tag.value[0], frames=tag.value[1])
    else:
        return tag


class ResultData(BaseModel):
    """
    Container for transferring results from the worker to the reducer. In enhances the pure payload with useful meta data

    Attributes:
        event_number: which event the result belongs to. NB: results may arrive out of order
        worker: which worker processed it.
        parameters_hash: which version of parameters was used to process the event
        payload: the data return from the custom worker function: process_event
    """

    event_number: EventNumber
    worker: WorkerName
    parameters_hash: Optional[HashDigest]
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
        ), f"Cannot merge data from events {[m.event_number for m in msgs]}"
        all_stream_names = [stream for m in msgs for stream in m.streams.keys()]
        assert len(all_stream_names) == len(
            set(all_stream_names)
        ), "Cannot merge data with duplicate streams"

        ret = EventData(event_number=msgs[0].event_number, streams={})
        for msg in msgs:
            ret.streams.update(msg.streams)
        return ret
