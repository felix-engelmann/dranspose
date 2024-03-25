import asyncio
import time
import os
import pickle
from typing import AsyncGenerator, Optional, Awaitable, Any, IO
from uuid import UUID

import redis.exceptions as rexceptions
import zmq.asyncio

from pydantic import UUID4, model_validator

from dranspose.distributed import DistributedService, DistributedSettings
from dranspose.event import StreamData, InternalWorkerMessage
from dranspose.helpers.utils import done_callback, cancel_and_wait
from dranspose.protocol import (
    IngesterState,
    StreamName,
    RedisKeys,
    WorkAssignment,
    IngesterName,
    ZmqUrl,
    ParameterName,
    ConnectedWorker,
    IngesterUpdate,
    DistributedStateEnum,
    WorkAssignmentList,
)


class IngesterSettings(DistributedSettings):
    ingester_streams: list[StreamName]
    ingester_name: IngesterName
    ingester_url: ZmqUrl = ZmqUrl("tcp://localhost:10000")
    dump_path: Optional[os.PathLike[Any] | str] = None

    @model_validator(mode="before")
    @classmethod
    def create_default_name(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "ingester_name" not in data and "ingester_streams" in data:
                if isinstance(data["ingester_streams"], list):
                    data["ingester_name"] = (
                        "_".join(map(str, data["ingester_streams"])) + "-ingester"
                    )
        return data


class Ingester(DistributedService):
    """
    The Ingester class provides a basis to write custom ingesters for any protocol.
    It handles all the forwarding to workers and managing assignments.
    For the instance to do anything, await `run()`
    """

    def __init__(self, settings: Optional[IngesterSettings] = None):
        if settings is None:
            settings = IngesterSettings()
        self._ingester_settings = settings
        state = IngesterState(
            name=self._ingester_settings.ingester_name,
            url=self._ingester_settings.ingester_url,
            streams=self._ingester_settings.ingester_streams,
        )

        super().__init__(state=state, settings=self._ingester_settings)
        self._logger.info(
            "created ingester with state %s and settings %s",
            state,
            self._ingester_settings,
        )
        self.state: IngesterState

        self.dump_file: Optional[IO[bytes]] = None

    def open_socket(self) -> None:
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
        self.open_socket()

        self.accept_task = asyncio.create_task(self.accept_workers())
        self.accept_task.add_done_callback(done_callback)
        self.work_task = asyncio.create_task(self.work())
        self.work_task.add_done_callback(done_callback)
        self.assign_task = asyncio.create_task(self.manage_assignments())
        self.assign_task.add_done_callback(done_callback)
        self.assignment_queue: asyncio.Queue[WorkAssignment] = asyncio.Queue()
        self.metrics_task = asyncio.create_task(self.update_metrics())
        self.metrics_task.add_done_callback(done_callback)
        self._logger.info("all subtasks running")
        await self.register()

    async def restart_work(self, new_uuid: UUID4) -> None:
        """
        Restarts all work related tasks to make sure no old state is present in a new scan.

        Arguments:
            new_uuid: The uuid of the new mapping
        """
        await cancel_and_wait(self.work_task)
        await cancel_and_wait(self.assign_task)
        self.state.mapping_uuid = new_uuid
        self.assignment_queue = asyncio.Queue()
        self.work_task = asyncio.create_task(self.work())
        self.work_task.add_done_callback(done_callback)
        self.assign_task = asyncio.create_task(self.manage_assignments())
        self.assign_task.add_done_callback(done_callback)

    async def finish_work(self) -> None:
        """
        This hook is called when all events of a trigger map were ingested. It is useful for e.g. closing open files.
        """
        self._logger.info("finishing work")
        if self.dump_file:
            self.dump_file.close()
            self._logger.info("closed dump file at finish %s", self.dump_file)
            self.dump_file = None

        await self.redis.xadd(
            RedisKeys.ready(self.state.mapping_uuid),
            {
                "data": IngesterUpdate(
                    state=DistributedStateEnum.FINISHED,
                    ingester=self.state.name,
                ).model_dump_json()
            },
        )

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
                was = WorkAssignmentList.validate_json(assignment[1]["data"])
                for wa in was:
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
        self.dump_filename = self._ingester_settings.dump_path
        if self.dump_filename is None and "dump_prefix" in self.parameters:
            val = self.parameters[ParameterName("dump_prefix")].data.decode("utf8")
            if len(val) > 0:
                self.dump_filename = f"{val}{self._ingester_settings.ingester_name}-{self.state.mapping_uuid}.pkls"
        if self.dump_filename:
            self.dump_file = open(self.dump_filename, "ab")
            self._logger.info(
                "dump file %s opened at %s",
                self.dump_filename,
                self.dump_file,
            )
        try:
            took = []
            empties = []
            while True:
                empty = False
                if self.assignment_queue.empty():
                    empty = True
                start = time.perf_counter()
                work_assignment: WorkAssignment = await self.assignment_queue.get()
                if empty:
                    empties.append(work_assignment.event_number)

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
                    self._logger.debug("writing dump to path %s", self.dump_filename)
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
                end = time.perf_counter()
                took.append(end - start)
                if len(took) > 1000:
                    self._logger.info(
                        "forwarding took avg %lf, min %f max %f",
                        sum(took) / len(took),
                        min(took),
                        max(took),
                    )
                    self._logger.info("waiting for queue %d of 1000", len(empties))
                    took = []
                    empties = []
                self.state.processed_events += 1
        except asyncio.exceptions.CancelledError:
            self._logger.info("stopping worker")
            for stream in self.state.streams:
                await self.stop_source(stream)
            if self.dump_file:
                self._logger.info(
                    "closing dump file %s at cancelled work", self.dump_file
                )
                self.dump_file.close()
                self.dump_file = None

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

    async def stop_source(self, stream: StreamName) -> None:
        pass

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
                connected_worker = ConnectedWorker(
                    name=data[0], service_uuid=UUID(bytes=data[1])
                )
                fast_publish = False
                if connected_worker.service_uuid not in self.state.connected_workers:
                    fast_publish = True
                self.state.connected_workers[
                    connected_worker.service_uuid
                ] = connected_worker
                now = time.time()
                self.state.connected_workers = {
                    uuid: cw
                    for uuid, cw in self.state.connected_workers.items()
                    if now - cw.last_seen < 3
                }
                self._logger.debug("worker pinnged %s", connected_worker)
                if fast_publish:
                    self._logger.debug("fast publish")
                    await self.publish_config()

    async def close(self) -> None:
        """
        Clean up any open connections
        """
        await cancel_and_wait(self.accept_task)
        await cancel_and_wait(self.work_task)
        await cancel_and_wait(self.metrics_task)
        await cancel_and_wait(self.assign_task)
        await self.redis.delete(RedisKeys.config("ingester", self.state.name))
        await super().close()
        self.ctx.destroy(linger=0)
        self._logger.info("closed ingester")
