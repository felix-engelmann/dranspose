import logging

import zmq

from dranspose.event import EventData

logger = logging.getLogger(__name__)


class RepubWorker:
    def __init__(self, context=None, **kwargs):
        print("context", context)
        if context is None:
            context = {}
            context["context"] = zmq.Context()
            context["socket"] = context["context"].socket(zmq.PUSH)
            context["socket"].bind("tcp://0.0.0.0:5556")

        self.sock = context["socket"]

    def process_event(self, event: EventData, parameters=None):
        logger.debug("using parameters %s", parameters)
        logger.debug("event %s", event.streams["eiger"])

        self.sock.send_multipart(event.streams["eiger"].frames)
