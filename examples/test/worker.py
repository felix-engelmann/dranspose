from typing import Any

from dranspose.event import EventData


class TestWorker:
    def __init__(self, **kwargs: Any) -> None:
        pass

    def process_event(self, event: EventData, *args, **kwargs):
        for stream in event.streams:
            for i, _ in enumerate(event.streams[stream].frames):
                event.streams[stream].frames[i] = event.streams[stream].frames[i][:5000]
        return event, args, kwargs
