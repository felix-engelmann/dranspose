import logging

from dranspose.event import ResultData
from dranspose.protocol import ReducerState


class ParamReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs):
        self.number = 0
        self.publish = {"params": {}}

    def process_result(self, result: ResultData, parameters=None):
        logging.info("parameters are %s", parameters)
        self.publish["params"] = {n: p.value for n, p in parameters.items()}
        self.publish["params"]["bytes_param"] = self.publish["params"][
            "bytes_param"
        ].decode()
        self.publish["params"]["bool_param"] = int(self.publish["params"]["bool_param"])
        logging.info("params %s", self.publish["params"])
        self.publish["worker_params"] = {n: p.value for n, p in result.payload.items()}
        self.publish["worker_params"]["bytes_param"] = self.publish["worker_params"][
            "bytes_param"
        ].decode()
        self.publish["worker_params"]["bool_param"] = int(
            self.publish["worker_params"]["bool_param"]
        )

    def finish(self, parameters=None):
        print("finished dummy reducer work")
