import logging

from dranspose.event import ResultData
from dranspose.protocol import ReducerState


class ParamReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs):
        self.number = 0
        self.publish = {"params": {}}

    def process_result(self, result: ResultData, parameters=None):
        logging.info("parameters are %s", parameters)
        self.publish["params"] = parameters
        self.publish["worker_params"] = result.payload

    def timer(self, parameters=None):
        logging.info("parameters are %s", parameters)
        self.publish["params"] = parameters

    def finish(self, parameters=None):
        logging.info("finished dummy reducer work")
