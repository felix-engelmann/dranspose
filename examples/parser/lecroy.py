import logging
from typing import Any

from dranspose.data.lecroy import LecroyParsed
from dranspose.event import EventData
from dranspose.middlewares.lecroy import parse
from dranspose.protocol import StreamName

logger = logging.getLogger(__name__)


class LecroyWorker:
    def __init__(self, *args: tuple[Any], **kwargs: dict[str, Any]) -> None:
        pass

    def process_event(
        self, event: EventData, *args: tuple[Any], **kwargs: dict[str, Any]
    ) -> None:
        if "lecroy" in event.streams:
            res = parse(event.streams[StreamName("lecroy")])
            if isinstance(res, LecroyParsed):
                for i, ch in enumerate(res.channels):
                    logger.info(f"looking at channel {ch}")
                    logger.info(f"meta packet is {res.meta[i]}")
                    logger.info(f"trace shape is {res.data[i].dtype}")
                    logger.info(f"timestamps list len is {len(res.timestamps[i])}")
                    logger.debug("traces %s", res.data[i])
                    logger.info("timestamps list len is %s", len(res.timestamps[i]))

            else:
                logger.info("control packet is %s", res)
