import asyncio
import json
import time
from typing import Any, Optional

import zmq.asyncio
import logging

from pydantic import UUID4, BaseModel, ConfigDict

from dranspose import protocol
import redis.asyncio as redis
import redis.exceptions as rexceptions

from dranspose.distributed import DistributedService, DistributedSettings
from dranspose.protocol import (
    WorkerState,
    RedisKeys,
    IngesterState,
    WorkerUpdate,
    WorkerStateEnum,
    WorkAssignment,
    WorkerName,
    EventNumber,
    IngesterName,
    StreamName,
)


class ConnectedIngester(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    socket: zmq.asyncio.Socket
    config: IngesterState


class WorkerSettings(DistributedSettings):
    pass


class Worker(DistributedService):
    def __init__(self, name: WorkerName, settings: Optional[WorkerSettings] = None):
        self._worker_settings = settings
        if self._worker_settings is None:
            self._worker_settings = WorkerSettings()

        state = WorkerState(name=name)
        super().__init__(state, self._worker_settings)
        self.state: WorkerState
        self.ctx = zmq.asyncio.Context()

        self._ingesters: dict[IngesterName, ConnectedIngester] = {}
        self._stream_map: dict[StreamName, zmq.Socket] = {}

    async def run(self) -> None:
        self.manage_ingester_task = asyncio.create_task(self.manage_ingesters())
        self.work_task = asyncio.create_task(self.work())
        await self.register()

    async def work(self) -> None:
        self._logger.info("started work task")

        await self.redis.xadd(
            RedisKeys.ready(self.state.mapping_uuid),
            {
                "data": WorkerUpdate(
                    state=WorkerStateEnum.IDLE,
                    new=True,
                    completed=EventNumber(0),
                    worker=self.state.name,
                ).model_dump_json()
            },
        )

        self._logger.info("registered ready message")

        lastev = "0"
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
            work_assignment = WorkAssignment.model_validate_json(assignments[1]["data"])
            ingesterset = set()
            for stream, workers in work_assignment.assignments.items():
                if self.state.name in workers:
                    try:
                        ingesterset.add(self._stream_map[stream])
                    except KeyError:
                        self._logger.error(
                            "ingester for stream %s not connected, available: %s",
                            stream,
                            self._ingesters,
                        )
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
                RedisKeys.ready(self.state.mapping_uuid),
                {
                    "data": WorkerUpdate(
                        state=WorkerStateEnum.IDLE,
                        completed=work_assignment.event_number,
                        worker=self.state.name,
                    ).model_dump_json()
                },
            )

    async def restart_work(self, new_uuid: UUID4) -> None:
        self._logger.info("resetting config %s", new_uuid)
        self.work_task.cancel()
        self.state.mapping_uuid = new_uuid
        self.work_task = asyncio.create_task(self.work())

    async def manage_ingesters(self) -> None:
        while True:
            configs = await self.redis.keys(RedisKeys.config("ingester"))
            processed = []
            for key in configs:
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

                await self._ingesters[iname].socket.send(b"")
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

    async def close(self) -> None:
        self.manage_ingester_task.cancel()
        await self.redis.delete(RedisKeys.config("worker", self.state.name))
        await self.redis.aclose()
        self.ctx.destroy()
        self._logger.info("worker closed")
