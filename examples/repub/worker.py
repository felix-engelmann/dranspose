import logging

import zmq

from dranspose.event import EventData

logger = logging.getLogger(__name__)


class RepubWorker:
    def __init__(self, context, state, **kwargs):
        logger.info("context is %s", context)
        logger.info("state is %s", state)
        if "context" not in context:
            context["context"] = zmq.Context()
            context["socket"] = context["context"].socket(zmq.PUSH)
            # hack to extract port from worker name
            port = int(state.name[1:])
            context["socket"].bind(f"tcp://0.0.0.0:{port}")

        self.sock = context["socket"]
        self.buffer = {}

    def process_event(self, event: EventData, parameters=None, *args, **kwargs):
        logger.debug("using parameters %s", parameters)
        logger.debug("event %s", event.streams["eiger"])

        self.buffer[event.event_number] = event

        self.sock.send_multipart(event.streams["eiger"].frames)
