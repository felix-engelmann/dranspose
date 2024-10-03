import json
import logging
import pickle

from dranspose.data.positioncap import PositionCapValues
from dranspose.event import EventData
from dranspose.middlewares import contrast
from dranspose.middlewares import xspress
from dranspose.middlewares import stream1
from dranspose.middlewares.positioncap import PositioncapParser
from dranspose.parameters import StrParameter, BinaryParameter

logger = logging.getLogger(__name__)


class FluorescenceWorker:
    def __init__(self, **kwargs):
        self.number = 0
        self.pcap = PositioncapParser()

    @staticmethod
    def describe_parameters():
        params = [
            StrParameter(name="roi1", default="bla"),
            StrParameter(name="file_parameter"),
            BinaryParameter(name="file_parameter_file"),
        ]
        return params

    def process_event(self, event: EventData, parameters=None, *args, **kwargs):
        logger.warning("using parameters %s", parameters)

        if "dummy" in event.streams:
            logger.info("got dummy data %s", event.streams["dummy"])
            temp = json.loads(event.streams["dummy"].frames[0].bytes)
            logger.info("temperature is %s", temp)
        if (
            "file_parameter_file" in parameters
            and parameters["file_parameter_file"].value != b""
        ):
            logger.debug("file is given %s", parameters["file_parameter_file"].value)
            arr = pickle.loads(parameters["file_parameter_file"].value)
            logger.info("array param %s", arr)

        if "pilatus" in event.streams:
            dat = stream1.parse(event.streams["pilatus"])
            logger.info("got pilatus frame %s", dat)

        if "pcap" in event.streams:
            logger.debug("raw pcap:%s", event.streams["pcap"].frames[0])
            res = self.pcap.parse(event.streams["pcap"])
            if isinstance(res, PositionCapValues):
                logger.info(
                    "got pcap values %s",
                    res,
                )
            else:
                logger.warning("got pcap %s", res)

        if {"contrast", "xspress3"} - set(event.streams.keys()) != set():
            logger.error(
                "missing streams for this worker, only present %s", event.streams.keys()
            )
            return
        try:
            con = contrast.parse(event.streams["contrast"])
        except Exception as e:
            logger.error("failed to parse contrast %s", e.__repr__())
            return

        try:
            spec = xspress.parse(event.streams["xspress3"])
        except Exception as e:
            logger.error("failed to parse xspress3 %s", e.__repr__())
            return
        logger.debug("contrast: %s", con)
        logger.debug("spectrum: %s", spec)

        roi_slice = json.loads(parameters["roi1"].data)

        if con.status == "running":
            # new data
            sx, sy = con.pseudo["x"][0], con.pseudo["y"][0]
            logger.debug("process position %s %s", sx, sy)

            roi1 = spec.data[-1][roi_slice[0] : roi_slice[1]].sum()

            return {"position": (sx, sy), "concentations": {"roi1": roi1}}
