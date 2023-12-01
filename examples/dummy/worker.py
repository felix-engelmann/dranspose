import logging

from dranspose.event import EventData
from dranspose.middlewares import contrast
from dranspose.middlewares import xspress

logger = logging.getLogger(__name__)


class FluorescenceWorker:
    def __init__(self, parameters=None):
        self.number = 0

    def process_event(self, event: EventData, parameters=None):
        logger.debug("using parameters %s", parameters)
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
        logger.error("contrast: %s", con)
        logger.error("spectrum: %s", spec)

        if con.status == "running":
            # new data
            sx, sy = con.pseudo["x"][0], con.pseudo["y"][0]
            logger.error("process position %s %s", sx, sy)

            roi1 = spec.data[3][parameters["roi1"][0] : parameters["roi1"][1]].sum()

            return {"position": (sx, sy), "concentations": {"roi1": roi1}}
