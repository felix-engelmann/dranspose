import logging
from collections import defaultdict

from dranspose.event import ResultData
from dranspose.protocol import ReducerState


class WriterReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs: dict) -> None:
        self.publish: dict[str, dict] = {"workers": {}, "frames": defaultdict(list)}

    def process_result(
        self, result: ResultData, parameters: dict | None = None
    ) -> None:
        logging.info("parameters are %s", parameters)
        wname = result.payload["worker_name"]
        logging.info("result is %s", result.payload)
        if "header" in result.payload:
            self.publish["workers"][wname] = result.payload["header"]
        elif "frame" in result.payload:
            info = result.payload["frame"]
            logging.info("frame info are %s", info)
            res = (info["frame_number"], info["position"])
            self.publish["frames"][wname].append(res)

    def finish(self, parameters: dict | None = None) -> None:
        print("finished dummy reducer work")
