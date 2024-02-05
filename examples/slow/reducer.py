import logging
import time

from dranspose.event import ResultData

logger = logging.getLogger(__name__)


class SlowReducer:
    def __init__(self, **kwargs):
        self.publish = {"map": {}}

    def process_result(self, result: ResultData, parameters=None):
        print(result)
        logger.info("processing_result %s", result)
        # if result.payload:
        time.sleep(0.5)
        logger.info("slept for 500ms")
        self.publish["map"][result.event_number] = 1
        logger.info("updated publish to %s", self.publish)

    def finish(self, parameters=None):
        print("finished dummy reducer work")
