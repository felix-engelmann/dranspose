import asyncio
import json
import time
from typing import Any

import zmq.asyncio
import logging

from pydantic import UUID4, BaseModel, ConfigDict

from dranspose import protocol
import redis.asyncio as redis
import redis.exceptions as rexceptions

from dranspose.distributed import DistributedService
from dranspose.protocol import WorkerState, RedisKeys, IngesterState, WorkerUpdate, WorkerStateEnum


class ConnectedIngester(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    socket: zmq.asyncio.Socket
    config: IngesterState


class Worker(DistributedService):
    def __init__(self, name: str, redis_host="localhost", redis_port=6379):
        super().__init__()
        self._logger = logging.getLogger(f"{__name__}+{name}")
        self.ctx = zmq.asyncio.Context()
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True, protocol=3
        )
        if ":" in name:
            raise Exception("Worker name must not contain a :")
        self.state = WorkerState(name=name)
        self._ingesters: dict[str, ConnectedIngester] = {}
        self._stream_map: dict[str, zmq.Socket] = {}

    async def run(self):
        self.manage_ingester_task = asyncio.create_task(self.manage_ingesters())
        self.work_task = asyncio.create_task(self.work())
        await self.register()

    async def work(self):
        self._logger.info("started work task")

        await self.redis.xadd(
            f"{protocol.PREFIX}:ready:{self.state.mapping_uuid}",
            WorkerUpdate(state=WorkerStateEnum.IDLE,
                         new=True,
                         completed=0,
                         worker=self.state.name).model_dump(mode="json")
        )

        lastev = 0
        proced = 0
        while True:
            sub = RedisKeys.assigned(self.state.mapping_uuid)
            try:
                assignments = await self.redis.xread({sub: lastev}, block=1000, count=1)
            except rexceptions.ConnectionError:
                break
            if sub not in assignments:
                continue
            assignments = assignments[sub][0][0]
            self._logger.debug("got assignments %s", assignments)
            self._logger.debug("stream map %s", self._stream_map)
            ingesterset = set()
            for stream, jsons in assignments[1].items():
                workers = json.loads(jsons)
                if self.state.name in workers:
                    try:
                        ingesterset.add(self._stream_map[stream])
                    except KeyError:
                        self._logger.error("ingester for stream %s not connected, available: %s", stream, self._ingesters)
            self._logger.debug("receive from ingesters %s", ingesterset)

            lastev = assignments[0]
            if len(ingesterset) == 0:
                continue
            tasks = [sock.recv_multipart() for sock in ingesterset]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            # print("done", done, "pending", pending)
            for res in done:
                self._logger.debug("received work %s", res.result()[0])
            proced += 1
            if proced % 500 == 0:
                self._logger.info("processed %d events", proced)
            await self.redis.xadd(
                f"{protocol.PREFIX}:ready:{self.state.mapping_uuid}",
                {"state": "idle", "completed": int(lastev.split("-")[0]), "worker": self.state.name},
            )

    async def restart_work(self, new_uuid: UUID4):
        self._logger.info("resetting config %s", new_uuid)
        self.work_task.cancel()
        self.state.mapping_uuid = new_uuid
        self.work_task = asyncio.create_task(self.work())


    async def manage_ingesters(self):
        while True:
            configs = await self.redis.keys(RedisKeys.config("ingester"))
            self._logger.debug("present_ingester_keys: %s", configs)
            processed = []
            for key in configs:
                self._logger.debug("processing ingester %s", key)
                cfg = IngesterState.model_validate_json(await self.redis.get(key))
                iname = cfg.name
                processed.append(iname)
                if (
                    iname in self._ingesters
                    and self._ingesters[iname].config.url != cfg.url
                ):
                    self._logger.warning(
                        "url of ingester changed from %s to %s, disconnecting",
                        self._ingesters[iname].config.url,
                        cfg.url,
                    )
                    self._ingesters[iname].socket.close()
                    del self._ingesters[iname]

                if iname not in self._ingesters:
                    self._logger.info("adding new ingester %s", iname)
                    sock = self.ctx.socket(zmq.DEALER)
                    sock.setsockopt(zmq.IDENTITY, self.state.name.encode("ascii"))
                    sock.connect(str(cfg.url))
                    await sock.send(b"")
                    self._ingesters[iname] = ConnectedIngester(config=cfg, socket=sock)
            for iname in set(self._ingesters.keys()) - set(processed):
                self._logger.info("removing stale ingester %s", iname)
                self._ingesters[iname].socket.close()
                del self._ingesters[iname]
            self._stream_map = {
                s: conn_ing.socket
                for ing, conn_ing in self._ingesters.items()
                for s in conn_ing.config.streams
            }
            self.state.ingesters = [a.config for a in self._ingesters.values()]

            await asyncio.sleep(2)

    async def close(self):
        self.manage_ingester_task.cancel()
        await self.redis.delete(f"{protocol.PREFIX}:worker:{self.state.name}:config")
        await self.redis.aclose()
        self.ctx.destroy()
        self._logger.info("worker closed")
