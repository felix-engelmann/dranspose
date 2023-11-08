import zmq.asyncio
import logging
from dranspose import protocol

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, control_url: str, name: bytes):
        self.ctx = zmq.asyncio.Context()
        self.ctrl_sock = self.ctx.socket(zmq.DEALER)
        self.ctrl_sock.setsockopt(zmq.IDENTITY, name)
        self.ctrl_sock.connect(control_url)

    async def run(self):
        await self.register()
        while True:
            data = await self.ctrl_sock.recv_multipart()
            logger.debug("received %s", data)

    async def register(self):
        await protocol.pong(self.ctrl_sock)

    def __del__(self):
        self.ctx.destroy()
        logger.info("stopped worker")