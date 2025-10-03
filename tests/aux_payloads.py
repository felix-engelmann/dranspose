import json
import logging
from typing import Any

import zmq

from dranspose.event import EventData, ResultData
from dranspose.protocol import ReducerState, WorkParameter, ParameterName


class TestWorker:
    def __init__(self, **kwargs: Any) -> None:
        pass

    def process_event(
        self, event: EventData, *args: tuple[Any, ...], **kwargs: dict[str, Any]
    ) -> tuple[EventData, tuple[Any, ...], dict[str, Any]]:
        for stream in event.streams:
            frame: zmq.Frame | bytes
            for i, frame in enumerate(event.streams[stream].frames):
                if isinstance(frame, zmq.Frame):
                    event.streams[stream].frames[i] = frame.bytes[:500]
                elif isinstance(frame, bytes):
                    event.streams[stream].frames[i] = frame[:500]
        return event, args, kwargs


class TestReducer:
    def __init__(
        self, state: ReducerState | None = None, **kwargs: dict[str, Any]
    ) -> None:
        self.publish: dict[str, dict[Any, Any]] = {"results": {}, "parameters": {}}

    def process_result(
        self,
        result: ResultData,
        parameters: dict[ParameterName, WorkParameter] | None = None,
    ) -> None:
        logging.info("parameters are %s", parameters)
        self.publish["results"][str(result.event_number)] = {
            k: json.loads(v.frames[0]) if v.typ == "STINS" else "blob"
            for k, v in result.payload[0].streams.items()
        }
        self.publish["parameters"][result.event_number] = parameters

    def finish(
        self, parameters: dict[ParameterName, WorkParameter] | None = None
    ) -> None:
        print("finished dummy reducer work")
