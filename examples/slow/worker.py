import logging
import time

from dranspose.event import EventData
from dranspose.parameters import FloatParameter

logger = logging.getLogger(__name__)


class SlowWorker:
    def __init__(self, **kwargs):
        pass

    @staticmethod
    def describe_parameters():
        params = [
            FloatParameter(name="sleep_time"),
        ]
        return params

    def process_event(self, event: EventData, parameters=None):
        logger.debug("using parameters %s", parameters)
        if "sleep_time" in parameters:
            time.sleep(float(parameters["sleep_time"]))
        else:
            time.sleep(0.28)
