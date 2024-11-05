import logging

from dranspose.data.eiger_legacy import EigerLegacyImage
from dranspose.event import EventData
from dranspose.middlewares.eiger_legacy import parse
from dranspose.protocol import StreamName

logger = logging.getLogger(__name__)


class LegacyWorker:
    def __init__(self, *args, **kwargs):
        pass

    def process_event(self, event: EventData, parameters=None, *args, **kwargs):
        if "eiger" in event.streams:
            data = parse(event.streams[StreamName("eiger")])
            if isinstance(data, EigerLegacyImage):
                data.buffer = b"some data removed"
            logger.info("parsed packet %s", data)
