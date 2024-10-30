import logging

from dranspose.event import ResultData


class ParamReducer:
    def __init__(self, state=None, **kwargs):
        self.number = 0
        self.publish = {"params": {}}

    def process_result(self, result: ResultData, parameters=None):
        logging.info("parameters are %s", parameters)
        self.publish["params"] = parameters
        self.publish["worker_params"] = result.payload

    def finish(self, parameters=None):
        print("finished dummy reducer work")
