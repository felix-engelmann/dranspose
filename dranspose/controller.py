import asyncio
import zmq.asyncio
import logging
import time
from dranspose import protocol
from dranspose.worker import WorkerState
import redis.asyncio as redis

from contextlib import asynccontextmanager
from fastapi import FastAPI


logger = logging.getLogger(__name__)


class Controller:
    def __init__(self, redis_host="localhost", redis_port=6379):
        self.ctx = zmq.asyncio.Context()
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True, protocol=3)

    async def run(self):
        logger.debug("started controller run")
        while True:
            presence = await self.redis.keys(f"{protocol.PREFIX}:*:*:present")
            await asyncio.sleep(2)

    async def close(self):
        self.ctx.destroy()
        await self.redis.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    ctrl = Controller()
    asyncio.create_task(ctrl.run())
    yield
    await ctrl.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)
