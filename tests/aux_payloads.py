import json
import logging
from typing import Any

from dranspose.event import EventData, ResultData
from dranspose.protocol import ReducerState


class TestWorker:
    def __init__(self, **kwargs: Any) -> None:
        pass

    def process_event(self, event: EventData, *args, **kwargs):
        for stream in event.streams:
            for i, _ in enumerate(event.streams[stream].frames):
                event.streams[stream].frames[i] = (
                    event.streams[stream].frames[i].bytes[:500]
                )
        return event, args, kwargs


class TestReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs: dict) -> None:
        self.publish: dict[str, dict] = {"results": {}, "parameters": {}}

    def process_result(
        self, result: ResultData, parameters: dict | None = None
    ) -> None:
        logging.info("parameters are %s", parameters)
        self.publish["results"][str(result.event_number)] = {
            k: json.loads(v.frames[0]) if v.typ == "STINS" else "blob"
            for k, v in result.payload[0].streams.items()
        }
        self.publish["parameters"][result.event_number] = parameters

    def finish(self, parameters: dict | None = None) -> None:
        print("finished dummy reducer work")
