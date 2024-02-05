import logging
import time

from dranspose.event import ResultData


class SlowReducer:
    def __init__(self, **kwargs):
        self.publish = {"map": {}}

    def process_result(self, result: ResultData, parameters=None):
        print(result)
        # if result.payload:
        time.sleep(0.5)
        self.publish["map"][result.event_number] = 1
        logging.info("updated publish to %s", self.publish)

    def finish(self, parameters=None):
        print("finished dummy reducer work")
