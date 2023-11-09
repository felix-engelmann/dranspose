import asyncio
import time
import zmq.asyncio
import logging
from dranspose import protocol
import redis.asyncio as redis

logger = logging.getLogger(__name__)

class WorkerState:
    def __init__(self, name):
        self.name = name


class Worker:
    def __init__(self, name: bytes, redis_host="localhost", redis_port=6379):
        self.ctx = zmq.asyncio.Context()
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True, protocol=3)
        self.state = WorkerState(name)
        self._ingesters = {}

    async def run(self):
        asyncio.create_task(self.register())
        asyncio.create_task(self.manage_ingesters())
        asyncio.create_task(self.work())

    async def work(self):
        while True:
            try:
                sock = list(self._ingesters.values())[0]["socket"]
                res = await sock.poll()
                print("awaited poll", res)
                res = await sock.recv_multipart()
                print("received work", res)
            except Exception as e:
                print("err ", e.__repr__())
                await asyncio.sleep(2)

    async def register(self):
        while True:
            await self.redis.setex(f"{protocol.PREFIX}:worker:{self.state.name.decode('ascii')}:present",10,1)
            await asyncio.sleep(6)

    async def manage_ingesters(self):
        while True:
            configs = await self.redis.keys(f"{protocol.PREFIX}:ingester:*:config")
            processed = []
            for key in configs:
                cfg = await self.redis.hgetall(key)
                iname = key.split(":")[2]
                processed.append(iname)
                if iname in self._ingesters and self._ingesters[iname]["config"]["url"] != cfg["url"]:
                    logger.warning("url of ingester changed from %s to %s, disconnecting", self._ingesters[iname]["config"]["url"],
                                   cfg["url"])
                    self._ingesters[iname]["socket"].close()
                    del self._ingesters[iname]

                if iname not in self._ingesters:
                    logger.info("adding new ingester %s", iname)
                    sock = self.ctx.socket(zmq.DEALER)
                    sock.setsockopt(zmq.IDENTITY, self.state.name)
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
        await self.redis.aclose()

    def __del__(self):
        self.ctx.destroy()
        logger.info("stopped worker")