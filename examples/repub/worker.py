import logging

import zmq

from dranspose.event import EventData

logger = logging.getLogger(__name__)


class RepubWorker:
    def __init__(self, context, **kwargs):
        print("context", context)
        if "context" not in context:
            context["context"] = zmq.Context()
            context["socket"] = context["context"].socket(zmq.PUSH)
            context["socket"].bind("tcp://0.0.0.0:5556")

        self.sock = context["socket"]
        self.buffer = {}

    def process_event(self, event: EventData, parameters=None):
        logger.debug("using parameters %s", parameters)
        logger.debug("event %s", event.streams["eiger"])

        self.buffer[event.event_number] = event

        self.sock.send_multipart(event.streams["eiger"].frames)
