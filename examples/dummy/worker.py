import json
import logging

from dranspose.event import EventData
from dranspose.middlewares import contrast
from dranspose.middlewares import xspress
from dranspose.middlewares import stream1
from dranspose.parameters import StrParameter, FileParameter

logger = logging.getLogger(__name__)


class FluorescenceWorker:
    def __init__(self, **kwargs):
        self.number = 0

    @staticmethod
    def describe_parameters():
        params = [
            StrParameter(name="roi1", default="bla"),
            FileParameter(name="file_parameter"),
        ]
        return params

    def process_event(self, event: EventData, parameters=None):
        logger.debug("using parameters %s", parameters)
        roi_slice = json.loads(parameters["roi1"].data)
        if "pilatus" in event.streams:
            dat = stream1.parse(event.streams["pilatus"])
            logger.info("got pilatus frame %s", dat)
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

        if con.status == "running":
            # new data
            sx, sy = con.pseudo["x"][0], con.pseudo["y"][0]
            logger.debug("process position %s %s", sx, sy)

            roi1 = spec.data[-1][roi_slice[0] : roi_slice[1]].sum()

            return {"position": (sx, sy), "concentations": {"roi1": roi1}}
