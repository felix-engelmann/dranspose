from dataclasses import dataclass
import logging
from typing import Any

import numpy as np

from dranspose.data.lecroy import LecroyParsed
from dranspose.event import EventData, ResultData
from dranspose.middlewares.lecroy import parse
from dranspose.parameters import IntParameter
from dranspose.protocol import ParameterName, StreamName, WorkParameter

logger = logging.getLogger(__name__)


@dataclass
class Result:
    max_val: np.typing.NDArray[Any]
    ts: list[float]


class LecroyWorker:
    def __init__(self, *args: tuple[Any], **kwargs: dict[str, Any]) -> None:
        logger.info("LecroyWorker init")

    @staticmethod
    def describe_parameters() -> list[IntParameter]:
        params = [
            IntParameter(name=ParameterName("channel"), default=2),
        ]
        return params

    def process_event(
        self,
        event: EventData,
        parameters: dict[ParameterName, WorkParameter],
        *args: tuple[Any],
        **kwargs: dict[str, Any],
    ) -> Result | None:
        logger.debug(f"LecroyWorker {event=}")
        if "lecroy" in event.streams:
            res = parse(event.streams[StreamName("lecroy")])
            if isinstance(res, LecroyParsed):
                for i, ch in enumerate(res.channels):
                    logger.info(f"looking at channel {ch}")
                    logger.info(f"meta packet is {res.meta[i]}")
                    logger.info(f"trace shape is {res.data[i].dtype}")
                    logger.info(f"timestamps list len is {len(res.timestamps[i])}")
                    logger.debug(f"traces {res.data[i]}")
                    logger.debug(f"timestamps list {res.timestamps[i]}")
                    if ch == parameters[ParameterName("channel")].value:
                        return Result(np.max(res.data[i], axis=1), res.timestamps[i])
            else:
                logger.info("control packet is %s", res)
        return None


class LecroyReducer:
    def __init__(
        self,
        parameters: dict[ParameterName, WorkParameter] | None = None,
        *args: tuple[Any],
        **kwargs: dict[str, Any],
    ) -> None:
        self.publish: dict[str, Any] = {"max_val": [], "ts": []}

    def process_result(
        self, result: ResultData, parameters: dict[ParameterName, WorkParameter]
    ) -> None:
        if result.payload and isinstance(result.payload, Result):
            self.publish["max_val"] = result.payload.max_val
            self.publish["ts"] = result.payload.ts
