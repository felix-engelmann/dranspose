import logging
from datetime import timedelta

from dranspose.data.positioncap import PositionCapValues
from dranspose.event import EventData
from dranspose.middlewares.positioncap import PositioncapParser

logger = logging.getLogger(__name__)


class PcapWorker:
    def __init__(self, *args, **kwargs):
        self.pcap = PositioncapParser()
        pass

    def process_event(self, event: EventData, parameters=None):
        if "pcap" in event.streams:
            res = self.pcap.parse(event.streams["pcap"])
            if isinstance(res, PositionCapValues):
                triggertime = timedelta(seconds=res.fields["PCAP.TS_TRIG.Value"].value)
                logger.info(
                    "got values %s at timestamp %s",
                    res,
                    self.pcap.arm_time + triggertime,
                )
