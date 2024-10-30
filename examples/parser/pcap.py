import logging

from dranspose.data.pcap import PCAPData
from dranspose.event import EventData
from dranspose.middlewares.pcap import parse

logger = logging.getLogger(__name__)


class PcapWorker:
    def __init__(self, *args, **kwargs):
        pass

    def process_event(self, event: EventData, *args, **kwargs):
        if "pcap" in event.streams:
            res = parse(event.streams["pcap"])
            if isinstance(res, PCAPData):
                logger.debug("parsed packet is %s", res)
            else:
                logger.info("control packet is %s", res)
