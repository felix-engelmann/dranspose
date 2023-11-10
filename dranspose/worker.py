import asyncio
import time
from typing import Any

import zmq.asyncio
import logging
from dranspose import protocol
import redis.asyncio as redis
import redis.exceptions as rexceptions


logger = logging.getLogger(__name__)


class WorkerState:
    def __init__(self, name):
        self.name = name
        self.mapping_uuid = None


class Worker:
    def __init__(self, name: str, redis_host="localhost", redis_port=6379):
        self.ctx = zmq.asyncio.Context()
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True, protocol=3
        )
        if ":" in name:
            raise Exception("Worker name must not container a :")
        self.state = WorkerState(name)
        self._ingesters: dict[str, Any] = {}

    async def run(self):
        asyncio.create_task(self.register())
        asyncio.create_task(self.manage_ingesters())
        asyncio.create_task(self.work())

    async def work(self):
        while True:
            try:
                tasks = [
                    a["socket"].recv_multipart() for a in list(self._ingesters.values())
                ]
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                # print("done", done, "pending", pending)
                for res in done:
                    print("received work", res.result())
            except Exception as e:
                print("err ", e.__repr__())
                await asyncio.sleep(2)

    async def register(self):
        latest = await self.redis.xrevrange(
            f"{protocol.PREFIX}:controller:updates", count=1
        )
        last = 0
        if len(latest) > 0:
            last = latest[0][0]
        while True:
            await self.redis.setex(
                f"{protocol.PREFIX}:worker:{self.state.name}:present", 10, 1
            )
            await self.redis.json().set(
                f"{protocol.PREFIX}:worker:{self.state.name}:config",
                "$",
                self.state.__dict__,
            )
            try:
                update = await self.redis.xread(
                    {f"{protocol.PREFIX}:controller:updates": last}, block=6000
                )
                if f"{protocol.PREFIX}:controller:updates" in update:
                    update = update[f"{protocol.PREFIX}:controller:updates"][0][-1]
                    last = update[0]
                    newuuid = update[1]["mapping_uuid"]
                    if newuuid != self.state.mapping_uuid:
                        logger.info("resetting config")
                        self.state.mapping_uuid = newuuid
            except rexceptions.ConnectionError as e:
                print("closing with", e.__repr__())
                break

    async def manage_ingesters(self):
        while True:
            configs = await self.redis.keys(f"{protocol.PREFIX}:ingester:*:config")
            processed = []
            for key in configs:
                cfg = await self.redis.json().get(key)
                iname = key.split(":")[2]
                processed.append(iname)
                if (
                    iname in self._ingesters
                    and self._ingesters[iname]["config"]["url"] != cfg["url"]
                ):
                    logger.warning(
                        "url of ingester changed from %s to %s, disconnecting",
                        self._ingesters[iname]["config"]["url"],
                        cfg["url"],
                    )
                    self._ingesters[iname]["socket"].close()
                    del self._ingesters[iname]

                if iname not in self._ingesters:
                    logger.info("adding new ingester %s", iname)
                    sock = self.ctx.socket(zmq.DEALER)
                    sock.setsockopt(zmq.IDENTITY, self.state.name.encode("ascii"))
                    try:
                        sock.connect(cfg["url"])
                        await sock.send(b"")
                        self._ingesters[iname] = {"config": cfg, "socket": sock}
                    except KeyError:
                        logger.error("invalid ingester config %s", cfg)
                        sock.close()
            for iname in set(self._ingesters.keys()) - set(processed):
                logger.info("removing stale ingester %s", iname)
                self._ingesters[iname]["socket"].close()
                del self._ingesters[iname]
            await asyncio.sleep(2)

    async def close(self):
        await self.redis.delete(f"{protocol.PREFIX}:worker:{self.state.name}:config")
        await self.redis.aclose()
        self.ctx.destroy()
