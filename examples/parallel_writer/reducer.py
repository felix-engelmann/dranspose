from collections import defaultdict
import logging

from dranspose.event import ResultData
from dranspose.protocol import ReducerState

logger = logging.getLogger(__name__)


class WriterReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs: dict) -> None:
        self.workers = {}
        self.frames = defaultdict(list)
        self.filename = ""
        self.publish: dict[str, dict] = {
            "workers": self.workers,
            "frames": self.frames,
            "filename": self.filename,
        }
        self.name = state.name

    def process_result(
        self, result: ResultData, parameters: dict | None = None
    ) -> None:
        logging.info("parameters are %s", parameters)
        wname = result.payload["worker_name"]
        logging.info("result is %s", result.payload)
        if "header" in result.payload:
            self.workers[wname] = result.payload["header"]
            self.filename = result.payload["header"]["master_filename"]
        elif "frame" in result.payload:
            info = result.payload["frame"]
            logging.info("frame info are %s", info)
            res = (info["frame_number"], info["position"])
            self.frames[wname].append(res)

    def finish(self, parameters: dict | None = None) -> None:
        pass
