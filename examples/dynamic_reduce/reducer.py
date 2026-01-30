import logging

from dranspose.event import ResultData
from dranspose.protocol import ReducerState


class InteractiveReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs):
        self.number = 0
        self.publish = {"params": {}}

    def process_result(self, result: ResultData, parameters=None):
        logging.info("parameters are %s", parameters)
        self.publish["params"] = {n: p.value for n, p in parameters.items()}

    def finish(self, parameters=None):
        print("finished dummy reducer work")
