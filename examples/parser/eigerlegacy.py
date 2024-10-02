import logging

from dranspose.event import EventData
from dranspose.middlewares.eiger_legacy import parse

logger = logging.getLogger(__name__)


class LegacyWorker:
    def __init__(self, *args, **kwargs):
        pass

    def process_event(self, event: EventData, parameters=None, *args, **kwargs):
        if "eiger" in event.streams:
            data = parse(event.streams["eiger"])
            logger.info("parsed packet %s", data)
