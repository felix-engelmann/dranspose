import json
import logging
import pickle
from typing import Any

from dranspose.data.positioncap import PositionCapValues
from dranspose.event import EventData
from dranspose.middlewares import contrast
from dranspose.middlewares import xspress
from dranspose.middlewares import stream1
from dranspose.middlewares.positioncap import PositioncapParser
from dranspose.parameters import StrParameter, BinaryParameter, ParameterBase
from dranspose.protocol import ParameterName, WorkParameter, StreamName

logger = logging.getLogger(__name__)


class FluorescenceWorker:
    def __init__(self, **kwargs: Any) -> None:
        self.number = 0
        self.pcap = PositioncapParser()

    @staticmethod
    def describe_parameters() -> list[ParameterBase]:
        params = [
            StrParameter(name=ParameterName("roi1"), default="bla"),
            StrParameter(name=ParameterName("file_parameter")),
            BinaryParameter(name=ParameterName("file_parameter_file")),
        ]
        return params

    def process_event(
        self,
        event: EventData,
        parameters: dict[ParameterName, WorkParameter] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        logger.warning("using parameters %s", parameters)

        if "dummy" in event.streams:
            logger.info("got dummy data %s", event.streams[StreamName("dummy")])
            temp = json.loads(event.streams[StreamName("dummy")].frames[0].bytes)
            logger.info("temperature is %s", temp)
        if (
            "file_parameter_file" in parameters
            and parameters[ParameterName("file_parameter_file")].value != b""
        ):
            logger.debug(
                "file is given %s",
                parameters[ParameterName("file_parameter_file")].value,
            )
            arr = pickle.loads(parameters[ParameterName("file_parameter_file")].value)
            logger.info("array param %s", arr)

        if "pilatus" in event.streams:
            dat = stream1.parse(event.streams[StreamName("pilatus")])
            logger.info("got pilatus frame %s", dat)

        if "pcap" in event.streams:
            logger.debug("raw pcap:%s", event.streams[StreamName("pcap")].frames[0])
            res = self.pcap.parse(event.streams[StreamName("pcap")])
            if isinstance(res, PositionCapValues):
                logger.info(
                    "got pcap values %s",
                    res,
                )
            else:
                logger.warning("got pcap %s", res)

        if {"contrast", "xspress3"} - set(event.streams.keys()) != set():
            logger.warning(
                "missing streams for this worker, only present %s", event.streams.keys()
            )
            return
        try:
            con = contrast.parse(event.streams[StreamName("contrast")])
        except Exception as e:
            logger.error("failed to parse contrast %s", e.__repr__())
            return

        try:
            spec = xspress.parse(event.streams[StreamName("xspress3")])
        except Exception as e:
            logger.error("failed to parse xspress3 %s", e.__repr__())
            return
        logger.debug("contrast: %s", con)
        logger.debug("spectrum: %s", spec)

        roi_slice = json.loads(parameters[("roi1")].data)

        if con.status == "running":
            # new data
            sx, sy = con.pseudo["x"][0], con.pseudo["y"][0]
            logger.debug("process position %s %s", sx, sy)

            roi1 = spec.data[-1][roi_slice[0] : roi_slice[1]].sum()

            return {"position": (sx, sy), "concentations": {"roi1": roi1}}
