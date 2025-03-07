import logging
from datetime import datetime, timezone
from typing import Any

from dranspose.event import ResultData
from dranspose.protocol import ReducerState, StreamName


class TimingReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs: Any) -> None:
        self.publish: dict[StreamName, list[tuple[float, float]]] = {}
        pass

    def process_result(self, result: ResultData, parameters: Any = None) -> None:
        print(result)
        logging.debug("result is %s", result.payload)
        if result.payload:
            now = datetime.now(timezone.utc)
            for stream in result.payload:
                if stream not in self.publish:
                    self.publish[stream] = []
                old, delta = result.payload[stream]
                total = (now - old).total_seconds()
                self.publish[stream].append((delta, total))
                logging.debug("result total delta was %s, workerdelta %s", total, delta)

    def finish(self, parameters: Any = None) -> None:
        print("finished dummy reducer work")
