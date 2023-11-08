import asyncio
import zmq.asyncio
import logging
import time
from dranspose import protocol

logger = logging.getLogger(__name__)


class WorkerState:
    def __init__(self, name):
        self.name = name
        self.last_seen = time.time()


class Controller:
    def __init__(self, control_bind="tcp://*:9999"):
        self.ctx = zmq.asyncio.Context()
        self.ctrl_sock = self.ctx.socket(zmq.ROUTER)
        self.ctrl_sock.bind(control_bind)
        self.workers = {}

    async def run(self):
        logger.debug("started controller run")
        asyncio.create_task(self.checklive())
        while True:
            data = await self.ctrl_sock.recv_multipart()
            logger.debug("received %s", data)

    async def checklive(self):
        logger.debug("started checklive")
        while True:
            for w in self.workers:
                await protocol.ping(self.ctrl_sock, self.workers[w].name)
            await asyncio.sleep(1)

    def __del__(self):
        self.ctx.destroy()
        logger.info("stopped controlled")