import time
import zmq.asyncio
import logging
from dranspose import protocol

logger = logging.getLogger(__name__)

class IngesterState:
    def __init__(self, name):
        self.name = name
        self.last_seen = time.time()

    def seen(self):
        self.last_seen = time.time()

    def dead(self):
        return (time.time() - self.last_seen) > 10

    def stale(self):
        return (time.time() - self.last_seen) > 5

class Ingester:
    def __init__(self, control_url: str, name: bytes):
        self.ctx = zmq.asyncio.Context()
        self.ctrl_sock = self.ctx.socket(zmq.DEALER)
        self.ctrl_sock.setsockopt(zmq.IDENTITY, name)
        self.ctrl_sock.connect(control_url)
        self.state = IngesterState(name)

    async def run(self):
        await self.register()
        while True:
            data = await self.ctrl_sock.recv_multipart()
            logger.debug("received %s", data)
            ret = await protocol.parse(data)
            if ret["type"] == protocol.Protocol.PING:
                await protocol.pong(self.ctrl_sock, self.state)

    async def register(self):
        await protocol.pong(self.ctrl_sock, self.state)

    def __del__(self):
        self.ctx.destroy()
        logger.info("stopped worker")