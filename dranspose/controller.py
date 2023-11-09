import asyncio
import zmq.asyncio
import logging
import time
from dranspose import protocol
from dranspose.worker import WorkerState

logger = logging.getLogger(__name__)


class Controller:
    def __init__(self, control_bind="tcp://*:9999"):
        self.ctx = zmq.asyncio.Context()
        self.ctrl_sock = self.ctx.socket(zmq.ROUTER)
        self.ctrl_sock.bind(control_bind)
        self.workers = {}
        self.ingesters = {}

    async def run(self):
        logger.debug("started controller run")
        asyncio.create_task(self.checklive())
        while True:
            data = await self.ctrl_sock.recv_multipart()
            logger.debug("received %s", data)
            if len(data) == 0:
                continue
            source = data[0]
            packet = await protocol.parse(data[1:])
            if packet["type"] == protocol.Protocol.PONG:
                packet["data"].seen()
                self.workers[source] = packet["data"]

    async def checklive(self):
        logger.debug("started checklive")
        while True:
            for w in self.workers:
                if self.workers[w].dead():
                    del self.workers[w]
                elif self.workers[w].stale():
                    await protocol.ping(self.ctrl_sock, self.workers[w].name)
            await asyncio.sleep(1)

    def __del__(self):
        self.ctx.destroy()
        logger.info("stopped controlled")