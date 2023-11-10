import asyncio
import zmq.asyncio
import logging
import time
from dranspose import protocol
from dranspose.mapping import Mapping
from dranspose.worker import WorkerState
import redis.asyncio as redis

from contextlib import asynccontextmanager
from fastapi import FastAPI


logger = logging.getLogger(__name__)


class ControllerState:
    def __init__(self):
        self.mapping_uuid = 0


class Controller:
    def __init__(self, redis_host="localhost", redis_port=6379):
        self.ctx = zmq.asyncio.Context()
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True, protocol=3
        )
        self.present = []
        self.configs = []
        self.state = ControllerState()

        self.mapping = Mapping()

    async def run(self):
        logger.debug("started controller run")
        asyncio.create_task(self.assign_work())
        while True:
            self.present = await self.redis.keys(f"{protocol.PREFIX}:*:*:present")
            self.configs = await self.redis.keys(f"{protocol.PREFIX}:*:*:config")
            await asyncio.sleep(2)

    async def set_mapping(self):
        self.mapping = Mapping()
        self.state.mapping_uuid = str(self.mapping.uuid)
        await self.redis.json().set(
            f"{protocol.PREFIX}:controller:config", "$", self.state.__dict__
        )
        await self.redis.xadd(
            f"{protocol.PREFIX}:controller:updates",
            {"mapping_uuid": self.state.mapping_uuid},
        )

        self.configs = await self.redis.keys(f"{protocol.PREFIX}:*:*:config")
        if len(self.configs) > 0:
            uuids = await self.redis.json().mget(self.configs, "$.mapping_uuid")
            while set([u[0] for u in uuids]) != {self.state.mapping_uuid}:
                await asyncio.sleep(0.05)
                uuids = await self.redis.json().mget(self.configs, "$.mapping_uuid")
        logger.info("new mapping distributed")

    async def assign_work(self):
        pass

    async def close(self):
        await self.redis.delete(f"{protocol.PREFIX}:controller:config")
        await self.redis.delete(f"{protocol.PREFIX}:controller:updates")
        self.ctx.destroy()
        await self.redis.close()

ctrl: Controller
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    global ctrl
    ctrl = Controller()
    asyncio.create_task(ctrl.run())
    yield
    await ctrl.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.post("/mapping")
async def set_mapping():
    await ctrl.set_mapping()
