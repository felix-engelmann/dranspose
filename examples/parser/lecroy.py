import logging

from dranspose.data.lecroy import LecroyData
from dranspose.event import EventData
from dranspose.middlewares.lecroy import parse

logger = logging.getLogger(__name__)


class LcapWorker:
    def __init__(self, *args, **kwargs):
        pass

    def process_event(self, event: EventData, *args, **kwargs):
        if "lecroy" in event.streams:
            res = parse(event.streams["lecroy"])
            if isinstance(res, LecroyData):
                logger.debug("parsed packet is %s", res)
            else:
                logger.info("control packet is %s", res)
