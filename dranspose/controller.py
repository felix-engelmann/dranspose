import asyncio
import json
import os
from typing import Dict, List

import uvicorn
import zmq.asyncio
import logging
import time
from dranspose import protocol
from dranspose.mapping import Mapping
import redis.asyncio as redis
import redis.exceptions as rexceptions

from contextlib import asynccontextmanager
from fastapi import FastAPI

logger = logging.getLogger(__name__)


class Controller:
    def __init__(self, redis_host="localhost", redis_port=6379):
        self.ctx = zmq.asyncio.Context()
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True, protocol=3
        )
        self.present = []
        self.configs = []

        self.mapping = Mapping({"":[]})

        self.completed = {}
        self.completed_events = []

    async def run(self):
        logger.debug("started controller run")
        self.assign_task = asyncio.create_task(self.assign_work())
        await self.monitor()

    async def monitor(self):
        while True:
            self.present = await self.redis.keys(f"{protocol.PREFIX}:*:*:present")
            self.configs = await self.redis.keys(f"{protocol.PREFIX}:*:*:config")
            await asyncio.sleep(2)

    async def set_mapping(self, m):
        self.assign_task.cancel()
        await self.redis.delete(f"{protocol.PREFIX}:ready:{self.mapping.uuid}")

        assignments = await self.redis.keys(
            f"{protocol.PREFIX}:assigned:{self.mapping.uuid}:*"
        )
        if len(assignments) > 0:
            await self.redis.delete(*assignments)

        # cleaned up
        self.mapping = m
        await self.redis.xadd(
            f"{protocol.PREFIX}:controller:updates",
            {"mapping_uuid": self.mapping.uuid},
        )

        self.configs = await self.redis.keys(f"{protocol.PREFIX}:*:*:config")
        if len(self.configs) > 0:
            uuids = await self.redis.json().mget(self.configs, "$.mapping_uuid")
            while set([u[0] for u in uuids]) != {self.mapping.uuid}:
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
                    {f"{protocol.PREFIX}:ready:{self.mapping.uuid}": last},
                    block=1000,
                )
                if f"{protocol.PREFIX}:ready:{self.mapping.uuid}" in workers:
                    for ready in workers[
                        f"{protocol.PREFIX}:ready:{self.mapping.uuid}"
                    ][0]:
                        logger.debug("got a ready worker %s", ready)
                        if ready[1]["state"] == "idle":
                            virt = self.mapping.assign_next(ready[1]["worker"])
                            if "new" not in ready[1]:
                                compev = int(ready[1]["completed"])
                                if compev not in self.completed:
                                    self.completed[compev] = []
                                self.completed[compev].append(ready[1]["worker"])
                                if (set([x for stream in self.mapping.get_event_workers(compev-1).values() for x in stream]) ==
                                        set(self.completed[compev])):
                                    self.completed_events.append(compev)
                            logger.debug(
                                "assigned worker %s to %s", ready[1]["worker"], virt
                            )
                            pipe = self.redis.pipeline()
                            for evn in range(event_no, self.mapping.complete_events):
                                wrks = self.mapping.get_event_workers(evn)
                                logger.debug("send out assignment %s", wrks)
                                await pipe.xadd(
                                    f"{protocol.PREFIX}:assigned:{self.mapping.uuid}",
                                    {s: json.dumps(w) for s, w in wrks.items()},
                                    id=evn + 1,
                                )
                                if evn % 1000 == 0:
                                    logger.info(
                                        "1000 events in %lf",
                                        time.perf_counter() - start,
                                    )
                                    start = time.perf_counter()
                            await pipe.execute()
                            event_no = self.mapping.complete_events
                        last = ready[0]
            except rexceptions.ConnectionError as e:
                break

    async def get_streams(self):
        if len(ctrl.configs) > 0:
            streams = await ctrl.redis.json().mget(ctrl.configs, "$.streams")
            return [x for s in streams if len(s) > 0 for x in s[0]]
        else:
            return []

    async def close(self):
        await self.redis.delete(f"{protocol.PREFIX}:controller:updates")
        queues = await self.redis.keys(f"{protocol.PREFIX}:ready:*")
        if len(queues) > 0:
            await self.redis.delete(*queues)
        assigned = await self.redis.keys(f"{protocol.PREFIX}:assigned:*")
        if len(assigned) > 0:
            await self.redis.delete(*assigned)
        self.ctx.destroy()
        await self.redis.aclose()


ctrl: Controller


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    global ctrl
    ctrl = Controller(redis_host=os.getenv("REDIS_HOST","localhost"), redis_port=os.getenv("REDIS_PORT",6379))
    run_task = asyncio.create_task(ctrl.run())
    yield
    run_task.cancel()
    await ctrl.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.get("/api/v1/streams")
async def get_streams():
    return await ctrl.get_streams()

@app.get("/api/v1/status")
async def get_status():
    return {"work_completed": ctrl.completed,
            "last_assigned": ctrl.mapping.complete_events,
            "assignment": ctrl.mapping.assignments,
            "completed_events": ctrl.completed_events,
            "finished": len(ctrl.completed_events) == ctrl.mapping.len()}

@app.post("/api/v1/mapping")
async def set_mapping(mapping: Dict[str, List[List[int]|None]]):
    try:
        streams = await ctrl.get_streams()
        if set(mapping.keys()) - set(streams) != set():
            return f"streams {set(mapping.keys()) - set(streams)} not available"
        m = Mapping(mapping)
        avail_workers = await ctrl.redis.keys(f"{protocol.PREFIX}:worker:*:config")
        if len(avail_workers) < m.min_workers():
            return f"only {len(avail_workers)} workers available, but {m.min_workers()} required"
        await ctrl.set_mapping(m)
        return m.uuid
    except Exception as e:
        return e.__repr__()