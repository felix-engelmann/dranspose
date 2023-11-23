import asyncio
import json
import os
import pickle
from typing import Coroutine, AsyncGenerator, Optional, Awaitable

import redis.exceptions as rexceptions
import redis.asyncio as redis
import zmq.asyncio
import logging

from pydantic import UUID4

from dranspose.distributed import DistributedService, DistributedSettings
from dranspose.event import StreamData, InternalWorkerMessage
from dranspose.protocol import (
    IngesterState,
    StreamName,
    RedisKeys,
    WorkAssignment,
    IngesterName,
    ZmqUrl,
)


class IngesterSettings(DistributedSettings):
    ingester_url: ZmqUrl = ZmqUrl("tcp://localhost:10000")
    dump_path: Optional[os.PathLike] = None


class Ingester(DistributedService):
    def __init__(self, name: IngesterName, settings: Optional[IngesterSettings] = None):
        self._ingester_settings = settings
        if self._ingester_settings is None:
            self._ingester_settings = IngesterSettings()
        streams: list[StreamName] = []
        state = IngesterState(
            name=name,
            url=self._ingester_settings.ingester_url,
            streams=streams,
        )

        super().__init__(state=state, settings=self._ingester_settings)
        self.state: IngesterState

        self.ctx = zmq.asyncio.Context()
        self.out_socket = self.ctx.socket(zmq.ROUTER)
        self.out_socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 300)
        self.out_socket.bind(f"tcp://*:{self._ingester_settings.ingester_url.port}")

    async def run(self) -> None:
        self.accept_task = asyncio.create_task(self.accept_workers())
        self.work_task = asyncio.create_task(self.work())
        self.assign_task = asyncio.create_task(self.manage_assignments())
        self.assignment_queue: asyncio.Queue[WorkAssignment] = asyncio.Queue()
        await self.register()

    async def restart_work(self, new_uuid: UUID4) -> None:
        self.work_task.cancel()
        self.assign_task.cancel()
        self.state.mapping_uuid = new_uuid
        self.assignment_queue = asyncio.Queue()
        self.work_task = asyncio.create_task(self.work())
        self.assign_task = asyncio.create_task(self.manage_assignments())

    async def finish_work(self) -> None:
        if self.dump_file:
            self.dump_file.close()
            self.dump_file = None

    async def manage_assignments(self) -> None:
        self._logger.info("started ingester manage assign task")
        lastev = 0
        while True:
            sub = RedisKeys.assigned(self.state.mapping_uuid)
            try:
                assignments = await self.redis.xread({sub: lastev}, block=1000)
            except rexceptions.ConnectionError:
                break
            if sub not in assignments:
                continue
            assignment_evs = assignments[sub][0]
            self._logger.debug("got assignments %s", assignment_evs)
            for assignment in assignment_evs:
                wa = WorkAssignment.model_validate_json(assignment[1]["data"])
                mywa = wa.get_workers_for_streams(self.state.streams)
                await self.assignment_queue.put(mywa)
                lastev = assignment[0]

    async def work(self) -> None:
        self._logger.info("started ingester work task")
        sourcegens = {stream: self.run_source(stream) for stream in self.state.streams}
        self.dump_file = None
        if self._ingester_settings.dump_path:
            self.dump_file = open(self._ingester_settings.dump_path, "ab")
        try:
            while True:
                work_assignment: WorkAssignment = await self.assignment_queue.get()
                workermessages = {}
                zmqyields: list[Awaitable[StreamData]] = []
                streams: list[StreamName] = []
                for stream in work_assignment.assignments:
                    zmqyields.append(anext(sourcegens[stream]))
                    streams.append(stream)
                zmqstreams: list[StreamData] = await asyncio.gather(*zmqyields)
                zmqparts: dict[StreamName, StreamData] = {
                    stream: zmqpart for stream, zmqpart in zip(streams, zmqstreams)
                }
                if self.dump_file:
                    self._logger.debug("writing dump to path %s", self._ingester_settings.dump_path)
                    allstr = InternalWorkerMessage(event_number=work_assignment.event_number,
                                                   streams = {k: v.get_bytes() for k,v in zmqparts.items()}
                                                   )
                    try:
                        pickle.dump(allstr, self.dump_file)
                    except Exception as e:
                        self._logger.error("cound not dump %s", e.__repr__())
                    self._logger.debug("written dump")
                for stream, workers in work_assignment.assignments.items():
                    for worker in workers:
                        if worker not in workermessages:
                            workermessages[worker] = InternalWorkerMessage(
                                event_number=work_assignment.event_number
                            )
                        workermessages[worker].streams[stream] = zmqparts[stream]
                self._logger.debug("workermessages %s", workermessages)
                for worker, message in workermessages.items():
                    self._logger.debug(
                        "header is %s",
                        message.model_dump_json(exclude={"streams": {"__all__": "frames"}}),
                    )
                    await self.out_socket.send_multipart(
                        [worker.encode("ascii")]
                        + [
                            message.model_dump_json(
                                exclude={"streams": {"__all__": "frames"}}
                            ).encode("utf8")
                        ]
                        + message.get_all_frames()
                    )
                    self._logger.debug("sent message to worker %s", worker)
        except asyncio.exceptions.CancelledError:
            self._logger.info("stopping worker")
            if self.dump_file:
                self.dump_file.close()

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        yield StreamData(typ="", frames=[])
        return

    async def accept_workers(self) -> None:
        poller = zmq.asyncio.Poller()
        poller.register(self.out_socket, zmq.POLLIN)
        while True:
            socks = dict(await poller.poll())
            for sock in socks:
                data = await sock.recv_multipart()
                self._logger.debug("new worker connected %s", data[0])

    async def close(self) -> None:
        self.accept_task.cancel()
        self.work_task.cancel()
        await self.redis.delete(RedisKeys.config("ingester", self.state.name))
        await self.redis.aclose()
        self.ctx.destroy(linger=0)
        self._logger.info("closed ingester")
