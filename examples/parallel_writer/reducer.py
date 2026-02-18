import logging

from dranspose.event import ResultData
from dranspose.protocol import ReducerState


class WriterReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs: dict) -> None:
        self.publish: dict[str, dict] = {"workers": []}

    def process_result(
        self, result: ResultData, parameters: dict | None = None
    ) -> None:
        logging.info("parameters are %s", parameters)
        if "header" in result.payload:
            self.publish["filename"] = result.payload["header"]["filename"]
            self.publish["workers"].append(result.payload["header"]["uuid"])

    def finish(self, parameters: dict | None = None) -> None:
        print("finished dummy reducer work")
