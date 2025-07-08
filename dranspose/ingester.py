import asyncio
import time
import os
import functools
from io import BufferedWriter
from typing import AsyncGenerator, Optional, Awaitable, Any, Iterator, Callable
from uuid import UUID
import logging

import cbor2
import redis.exceptions as rexceptions
import zmq.asyncio
from cbor2 import CBORTag

from pydantic import UUID4, model_validator

from dranspose.distributed import DistributedService, DistributedSettings
from dranspose.event import StreamData, InternalWorkerMessage, message_encoder
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
    WorkerName,
)

class Dumper:
    def __init__(self, dump_path: str | None) -> None:
        self.dump_path = dump_path
        self.fh: BufferedWriter | None = None
        self._logger = logging.getLogger("dumper")

    def write_dump(self, message: Callable[[], Any] | Any) -> None:
        if self.dump_path is None:
            return
        if callable(message):
            message = message()
        if not self.fh:
            self.fh = open(self.dump_path, "ba")
            self._logger.info("dump file %s opened", self.dump_path)
        self._logger.debug("writing dump to %s", self.dump_path)
        try:
            cbor2.dump(
                CBORTag(55799, message),
                self.fh,
                default=message_encoder,
            )
            self.fh.flush()
            os.fsync(self.fh.fileno())
        except Exception as e:
            self._logger.error("cound not dump %s", e.__repr__())
        self._logger.debug("written dump")

    def __del__(self) -> None:
        if self.fh:
            self.fh.close()


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


class IsSoftwareTriggered(Exception):
    pass


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
        self.active_streams: list[StreamName] = []

    def _final_dump_path(self) -> str | None:
        """ Determines the filepath to write the dump to, if one should be written. Returns None otherwise. """
        if ret := self._ingester_settings.dump_path:
            return str(ret)
        if "dump_prefix" in self.parameters:
            val = self.parameters[ParameterName("dump_prefix")].data.decode("utf8")
            if len(val) > 0:
                return f"{val}{self._ingester_settings.ingester_name}-{self.state.mapping_uuid}.cbors"
        return None

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
        try:
            self.open_socket()
        except Exception as e:
            self._logger.error(
                "unable to open inward facing router socket %s", e.__repr__()
            )

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

    async def restart_work(
        self, new_uuid: UUID4, active_streams: list[StreamName]
    ) -> None:
        """
        Restarts all work related tasks to make sure no old state is present in a new scan.

        Arguments:
            new_uuid: The uuid of the new mapping
        """
        await cancel_and_wait(self.work_task)
        await cancel_and_wait(self.assign_task)
        self.state.mapping_uuid = new_uuid
        self.active_streams = list(set(active_streams).intersection(self.state.streams))
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
                    mywa = wa.get_workers_for_streams(self.active_streams)
                    if len(mywa.assignments) > 0:
                        await self.assignment_queue.put(mywa)
                lastev = assignment[0]

    async def _send_workermessages(
        self, workermessages: dict[WorkerName, InternalWorkerMessage]
    ) -> None:
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

    async def _get_zmqparts(
        self,
        work_assignment: WorkAssignment,
        sourcegens: dict[
            StreamName, AsyncGenerator[StreamData | IsSoftwareTriggered, None]
        ],
        swtriggen: Iterator[dict[StreamName, StreamData]] | None,
    ) -> dict[StreamName, StreamData]:
        zmqyields: list[Awaitable[StreamData | IsSoftwareTriggered]] = []
        streams: list[StreamName] = []
        for stream in work_assignment.assignments:
            zmqyields.append(anext(sourcegens[stream]))
            streams.append(stream)
        try:
            zmqstreams: list[StreamData | IsSoftwareTriggered] = await asyncio.gather(
                *zmqyields
            )
        except StopAsyncIteration:
            self._logger.warning("stream source stopped before end")
            raise asyncio.exceptions.CancelledError()
        zmqparts: dict[StreamName, StreamData | IsSoftwareTriggered] = {
            stream: zmqpart for stream, zmqpart in zip(streams, zmqstreams)
        }
        self._logger.debug("stream triggered zmqparts %s", zmqparts)
        if swtriggen is not None:
            swparts = next(swtriggen)
            zmqparts.update(swparts)
            # that has to overwrite all IsSoftwareTriggered instance

        return zmqparts

    async def work(self) -> None:
        """
        The heavy liftig function of an ingester. It consumes a generator `run_source()` which
        should be implemented for a specific protocol.
        It then assembles all streams for this ingester and forwards them to the assigned workers.

        Optionally the worker dumps the internal messages to disk. This is useful for development of workers with actual data captured.
        """
        self._logger.info("started ingester work task")
        dumper = Dumper(self._final_dump_path())
        sourcegens = {stream: self.run_source(stream) for stream in self.active_streams}
        if len(sourcegens) == 0:
            self._logger.warning("this ingester has no active streams, stopping worker")
            return
        swtriggen: Iterator[dict[StreamName, StreamData]] | None = getattr(self, "software_trigger", lambda: None)()
        time_spent_per_assignment = []
        time_spent_waiting = 0.0
        try:
            while True:
                waiting = self.assignment_queue.empty()
                start_time = time.perf_counter()
                work_assignment: WorkAssignment = await self.assignment_queue.get()
                if waiting:
                    time_spent_waiting += time.perf_counter() - start_time
                zmqparts = await self._get_zmqparts(
                    work_assignment, sourcegens, swtriggen
                )
                dumper.write_dump(lambda: InternalWorkerMessage(
                        event_number=work_assignment.event_number,
                        streams={k: v.get_bytes() for k, v in zmqparts.items()},
                        )
                    )
                workermessages: dict[WorkerName, InternalWorkerMessage] = {}
                for stream, workers in work_assignment.assignments.items():
                    for worker in workers:
                        if worker not in workermessages:
                            workermessages[worker] = InternalWorkerMessage(
                                event_number=work_assignment.event_number
                            )
                        workermessages[worker].streams[stream] = zmqparts[stream]
                self._logger.debug("workermessages %s", workermessages)
                await self._send_workermessages(workermessages)
                end = time.perf_counter()
                time_spent_per_assignment.append(end - start_time)
                if len(time_spent_per_assignment) > 1000:
                    self._logger.info(
                        "forwarding took avg %lf, min %f max %f. waiting time %f%",
                        sum(time_spent_per_assignment) / len(time_spent_per_assignment),
                        min(time_spent_per_assignment),
                        max(time_spent_per_assignment),
                        time_spent_waiting / sum(time_spent_per_assignment) * 100
                    )
                    # reset counters
                    time_spent_per_assignment = []
                    time_spent_waiting = 0
                self.state.processed_events += 1
        except asyncio.exceptions.CancelledError:
            self._logger.info("stopping worker")
            for stream in self.active_streams:
                await self.stop_source(stream)
            del dumper

    async def run_source(
        self, stream: StreamName
    ) -> AsyncGenerator[StreamData | IsSoftwareTriggered, None]:
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
            socks = dict(await poller.poll(timeout=1))
            # clean up old workers
            now = time.time()
            self.state.connected_workers = {
                uuid: cw
                for uuid, cw in self.state.connected_workers.items()
                if now - cw.last_seen < 4
            }
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
