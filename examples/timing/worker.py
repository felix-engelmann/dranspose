import logging
from datetime import datetime, timezone

from dranspose.event import EventData
from dranspose.middlewares import stream1

logger = logging.getLogger(__name__)


class TimingWorker:
    def __init__(self, **kwargs):
        pass

    def process_event(
        self, event: EventData, parameters=None, tick=False, *args, **kwargs
    ):
        logger.debug("using parameters %s", parameters)
        logger.info("tick %s", tick)
        times = {}
        for stream in event.streams:
            if stream in ["large", "fast"]:
                dat = stream1.parse(event.streams[stream])
                if hasattr(dat, "timestamps"):
                    ts = dat.timestamps
                    now = datetime.now(timezone.utc)
                    oldest = min(map(lambda x: datetime.fromisoformat(x), ts.values()))
                    delta = now - oldest
                    logger.debug("delta is %s", delta.total_seconds())
                    times[stream] = (oldest, delta.total_seconds())
        return times

    def get_tick_interval(self, parameters=None):
        return 1
