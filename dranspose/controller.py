import asyncio
import json

import zmq.asyncio
import logging
import time
from dranspose import protocol
from dranspose.mapping import Mapping
from dranspose.worker import WorkerState
import redis.asyncio as redis
import redis.exceptions as rexceptions

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

        logger.debug("started controller run")
        self.assign_task = asyncio.create_task(self.assign_work())

        asyncio.create_task(self.monitor())

    async def monitor(self):
        while True:
            self.present = await self.redis.keys(f"{protocol.PREFIX}:*:*:present")
            self.configs = await self.redis.keys(f"{protocol.PREFIX}:*:*:config")
            await asyncio.sleep(2)

    async def set_mapping(self):
        self.assign_task.cancel()
        await self.redis.delete(f"{protocol.PREFIX}:ready:{self.state.mapping_uuid}")
        self.state.mapping_uuid = str(self.mapping.uuid)

        assignments = await self.redis.keys(
            f"{protocol.PREFIX}:assigned:{self.state.mapping_uuid}:*"
        )
        if len(assignments) > 0:
            await self.redis.delete(*assignments)

        # cleaned up
        self.mapping = Mapping()
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
        self.assign_task = asyncio.create_task(self.assign_work())

    async def assign_work(self):
        last = 0
        event_no = 0
        start = time.perf_counter()
        while True:
            try:
                workers = await self.redis.xread(
                    {f"{protocol.PREFIX}:ready:{self.state.mapping_uuid}": last},
                    block=1000,
                )
                if f"{protocol.PREFIX}:ready:{self.state.mapping_uuid}" in workers:
                    for ready in workers[
                        f"{protocol.PREFIX}:ready:{self.state.mapping_uuid}"
                    ][0]:
                        logger.debug("got a ready worker %s", ready)
                        if ready[1]["state"] == "idle":
                            virt = self.mapping.assign_next(ready[1]["worker"])
                            logger.debug(
                                "assigned worker %s to &s", ready[1]["worker"], virt
                            )
                            for evn in range(event_no, self.mapping.complete_events):
                                wrks = self.mapping.get_event_workers(evn)
                                logger.debug("send out assignment %s", wrks)
                                await self.redis.xadd(
                                    f"{protocol.PREFIX}:assigned:{self.state.mapping_uuid}",
                                    {s: json.dumps(w) for s, w in wrks.items()},
                                    id=evn + 1,
                                )
                                if evn % 1000 == 0:
                                    logger.info(
                                        "1000 events in %lf",
                                        time.perf_counter() - start,
                                    )
                                    start = time.perf_counter()
                            event_no = self.mapping.complete_events
                        last = ready[0]
            except rexceptions.ConnectionError as e:
                break

    async def close(self):
        await self.redis.delete(f"{protocol.PREFIX}:controller:config")
        await self.redis.delete(f"{protocol.PREFIX}:controller:updates")
        queues = await self.redis.keys(f"{protocol.PREFIX}:ready:*")
        await self.redis.delete(*queues)
        assigned = await self.redis.keys(f"{protocol.PREFIX}:assigned:*")
        await self.redis.delete(*assigned)
        self.ctx.destroy()
        await self.redis.close()


ctrl: Controller


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    global ctrl
    ctrl = Controller()
    yield
    await ctrl.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.post("/mapping")
async def set_mapping():
    await ctrl.set_mapping()
