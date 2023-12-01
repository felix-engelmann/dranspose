import asyncio
import json
import os
import pickle
from typing import Coroutine, AsyncGenerator, Optional, Awaitable, Any, IO

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
    dump_path: Optional[os.PathLike[Any]] = None


class Ingester(DistributedService):
    """
    The Ingester class provides a basis to write custom ingesters for any protocol.
    It handles all the forwarding to workers and managing assignments.
    For the instance to do anything, await `run()`
    """

    def __init__(self, name: IngesterName, settings: Optional[IngesterSettings] = None):
        if settings is None:
            settings = IngesterSettings()
        self._ingester_settings = settings
        streams: list[StreamName] = []
        state = IngesterState(
            name=name,
            url=self._ingester_settings.ingester_url,
            streams=streams,
        )

        super().__init__(state=state, settings=self._ingester_settings)
        self.state: IngesterState

        self.dump_file: Optional[IO[bytes]] = None

        self.ctx = zmq.asyncio.Context()
        self.out_socket = self.ctx.socket(zmq.ROUTER)
        self.out_socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 300)
        self.out_socket.bind(f"tcp://*:{self._ingester_settings.ingester_url.port}")

    async def run(self) -> None:
        """
        Main function orchestrating the dependent tasks. This needs to be called from an async context once an instance is created.
        """
        self.accept_task = asyncio.create_task(self.accept_workers())
        self.work_task = asyncio.create_task(self.work())
        self.assign_task = asyncio.create_task(self.manage_assignments())
        self.assignment_queue: asyncio.Queue[WorkAssignment] = asyncio.Queue()
        await self.register()

    async def restart_work(self, new_uuid: UUID4) -> None:
        """
        Restarts all work related tasks to make sure no old state is present in a new scan.

        Arguments:
            new_uuid: The uuid of the new mapping
        """
        self.work_task.cancel()
        self.assign_task.cancel()
        self.state.mapping_uuid = new_uuid
        self.assignment_queue = asyncio.Queue()
        self.work_task = asyncio.create_task(self.work())
        self.assign_task = asyncio.create_task(self.manage_assignments())

    async def finish_work(self) -> None:
        """
        This hook is called when all events of a trigger map were ingested. It is useful for e.g. closing open files.
        """
        self._logger.info("finishing work")
        if self.dump_file:
            self.dump_file.close()
            self._logger.info("closed dump file at finish %s", self.dump_file)
            self.dump_file = None

    async def manage_assignments(self) -> None:
        """
        A background task reading assignments from the controller, filering them for the relevant ones and enqueueing them for when the frame arrives.
        """
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
        """
        The heavy liftig function of an ingester. It consumes a generator `run_source()` which
        should be implemented for a specific protocol.
        It then assembles all streams for this ingester and forwards them to the assigned workers.

        Optionally the worker dumps the internal messages to disk. This is useful to develop workers with actual data captured.
        """
        self._logger.info("started ingester work task")
        sourcegens = {stream: self.run_source(stream) for stream in self.state.streams}
        self.dump_file = None
        if self._ingester_settings.dump_path:
            self.dump_file = open(self._ingester_settings.dump_path, "ab")
            self._logger.info(
                "dump file %s opened at %s",
                self._ingester_settings.dump_path,
                self.dump_file,
            )
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
                    self._logger.debug(
                        "writing dump to path %s", self._ingester_settings.dump_path
                    )
                    allstr = InternalWorkerMessage(
                        event_number=work_assignment.event_number,
                        streams={k: v.get_bytes() for k, v in zmqparts.items()},
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
                        message.model_dump_json(
                            exclude={"streams": {"__all__": "frames"}}
                        ),
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
                self._logger.info(
                    "closing dump file %s at cancelled work", self.dump_file
                )
                self.dump_file.close()

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        """
        This generator must be implemented by the customised subclass. It should return exactly one `StreamData` object
        for every frame arriving from upstream.

        Arguments:
            stream: optionally it received a stream name for which is should yield frames.

        Returns:
            Yield a StreamData object for every received frame.
        """
        yield StreamData(typ="", frames=[])
        return

    async def accept_workers(self) -> None:
        """
        To allow zmq to learn the names of attached workers, they periodically send empty packets.
        There is no information flow directly from workers to ingesters, so we discard the data.
        """
        poller = zmq.asyncio.Poller()
        poller.register(self.out_socket, zmq.POLLIN)
        while True:
            socks = dict(await poller.poll())
            for sock in socks:
                data = await sock.recv_multipart()
                self._logger.debug("new worker connected %s", data[0])

    async def close(self) -> None:
        """
        Clean up any open connections
        """
        self.accept_task.cancel()
        self.work_task.cancel()
        await self.redis.delete(RedisKeys.config("ingester", self.state.name))
        await super().close()
        self.ctx.destroy(linger=0)
        self._logger.info("closed ingester")
