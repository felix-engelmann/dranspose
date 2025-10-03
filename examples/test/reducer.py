import logging

from dranspose.event import ResultData
from dranspose.protocol import ReducerState


class TestReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs: dict) -> None:
        self.publish: dict[str, dict] = {"results": {}, "parameters": {}}

    def process_result(
        self, result: ResultData, parameters: dict | None = None
    ) -> None:
        logging.info("parameters are %s", parameters)
        # logging.warning("payload %s", result.payload[0].model_dump())
        self.publish["results"][str(result.event_number)] = result.payload
        self.publish["parameters"][result.event_number] = parameters

    def finish(self, parameters: dict | None = None) -> None:
        print("finished dummy reducer work")
