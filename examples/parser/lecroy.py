import logging
from typing import Any

from dranspose.data.lecroy import LecroyData
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
            if isinstance(res, LecroyData):
                logger.debug("parsed packet is %s", res)
            else:
                logger.info("control packet is %s", res)
