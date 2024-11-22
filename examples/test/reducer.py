import logging

from dranspose.event import ResultData


class TestReducer:
    def __init__(self, state=None, **kwargs):
        self.publish = {"results": {}, "parameters": {}}

    def process_result(self, result: ResultData, parameters=None):
        logging.info("parameters are %s", parameters)
        self.publish["results"][result.event_number] = result.payload
        self.publish["parameters"][result.event_number] = parameters

    def finish(self, parameters=None):
        print("finished dummy reducer work")
