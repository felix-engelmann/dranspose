import asyncio
import json
import pickle
import random
import socket
import string
import time
import traceback
from asyncio import Future
from typing import Optional, Any

import zmq.asyncio

from pydantic import UUID4, BaseModel, ConfigDict, Field

from dranspose.helpers import utils
import redis.exceptions as rexceptions

from dranspose.distributed import DistributedService, DistributedSettings
from dranspose.event import InternalWorkerMessage, EventData, ResultData
from dranspose.helpers.utils import done_callback
from dranspose.protocol import (
    WorkerState,
    RedisKeys,
    IngesterState,
    WorkerUpdate,
    DistributedStateEnum,
    WorkAssignment,
    WorkerName,
    EventNumber,
    IngesterName,
    StreamName,
    ReducerState,
    WorkerTimes,
    GENERIC_WORKER,
    WorkerTag,
)


class RedisException(Exception):
    pass


class ConnectedIngester(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    socket: zmq.asyncio.Socket
    config: IngesterState
    pinged_since: float = Field(default_factory=time.time)


def random_worker_name() -> WorkerName:
    randid = "".join([random.choice(string.ascii_letters) for _ in range(10)])
    name = "Worker-{}-{}".format(socket.gethostname(), randid)
    return WorkerName(name)


class WorkerSettings(DistributedSettings):
    worker_class: Optional[str] = None
    worker_name: WorkerName = Field(default_factory=random_worker_name)
    worker_tags: set[WorkerTag] = {GENERIC_WORKER}


class Worker(DistributedService):
    def __init__(self, settings: Optional[WorkerSettings] = None):
        self._worker_settings = settings
        if self._worker_settings is None:
            self._worker_settings = WorkerSettings()

        state = WorkerState(
            name=self._worker_settings.worker_name,
            tags=self._worker_settings.worker_tags,
        )
        super().__init__(state, self._worker_settings)
        self._logger.info("created worker with state %s", state)
        self.state: WorkerState
        self.ctx = zmq.asyncio.Context()

        self._ingesters: dict[IngesterName, ConnectedIngester] = {}
        self._stream_map: dict[StreamName, zmq._future._AsyncSocket] = {}
        self.poll_task: Optional[Future[list[int]]] = None

        self._reducer_service_uuid: Optional[UUID4] = None
        self.out_socket: Optional[zmq._future._AsyncSocket] = None

        self.param_descriptions = []
        self.custom = None
        self.custom_context: dict[Any, Any] = {}
        self.worker = None
        if self._worker_settings.worker_class:
            try:
                self.custom = utils.import_class(self._worker_settings.worker_class)
                self._logger.info("custom worker class %s", self.custom)

                try:
                    self.param_descriptions = self.custom.describe_parameters()  # type: ignore[attr-defined]
                except AttributeError:
                    self._logger.info(
                        "custom worker class has no describe_parameters staticmethod"
                    )
                except Exception as e:
                    self._logger.error(
                        "custom worker parameter descripition is broken: %s",
                        e.__repr__(),
                    )

            except Exception as e:
                self._logger.error(
                    "no custom worker class loaded, discarding events, err %s\n%s",
                    e.__repr__(),
                    traceback.format_exc(),
                )

    async def run(self) -> None:
        self.manage_ingester_task = asyncio.create_task(self.manage_ingesters())
        self.manage_ingester_task.add_done_callback(done_callback)
        self.manage_receiver_task = asyncio.create_task(self.manage_receiver())
        self.manage_receiver_task.add_done_callback(done_callback)
        self.work_task = asyncio.create_task(self.work())
        self.work_task.add_done_callback(done_callback)
        self.metrics_task = asyncio.create_task(self.update_metrics())
        self.metrics_task.add_done_callback(done_callback)
        await self.register()

    async def notify_worker_ready(self) -> None:
        await self.redis.xadd(
            RedisKeys.ready(self.state.mapping_uuid),
            {
                "data": WorkerUpdate(
                    state=DistributedStateEnum.IDLE,
                    new=True,
                    completed=EventNumber(0),
                    worker=self.state.name,
                ).model_dump_json()
            },
        )

        self._logger.info("registered ready message")

    async def get_new_assignments(
        self, lastev: str
    ) -> tuple[Optional[str], Optional[set[zmq._future._AsyncSocket]]]:
        sub = RedisKeys.assigned(self.state.mapping_uuid)
        try:
            assignments = await self.redis.xread({sub: lastev}, block=1000, count=1)
        except rexceptions.ConnectionError:
            raise RedisException()
        if sub not in assignments:
            return None, None
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
        return lastev, ingesterset

    async def poll_internals(
        self, ingesterset: set[zmq._future._AsyncSocket]
    ) -> list[int]:
        self._logger.debug("poll internal sockets %s", ingesterset)
        poll_tasks = [sock.poll() for sock in ingesterset]
        self._logger.debug("await poll tasks %s", poll_tasks)
        self.poll_task = asyncio.gather(*poll_tasks)
        done = await self.poll_task
        self._logger.debug("data is available done: %s", done)
        return done

    async def build_event(
        self, ingesterset: set[zmq._future._AsyncSocket]
    ) -> EventData:
        msgs = []
        for sock in ingesterset:
            res = await sock.recv_multipart(copy=False)
            prelim = json.loads(res[0].bytes)
            pos = 1
            for stream, data in prelim["streams"].items():
                data["frames"] = res[pos : pos + data["length"]]
                pos += data["length"]
            msg = InternalWorkerMessage.model_validate(prelim)
            msgs.append(msg)

        return EventData.from_internals(msgs)

    async def work(self) -> None:
        self._logger.info("started work task")

        self.worker = None
        if self.custom:
            try:
                self.worker = self.custom(
                    parameters=self.parameters,
                    context=self.custom_context,
                    state=self.state,
                )
            except Exception as e:
                self._logger.error(
                    "Failed to instantiate custom worker: %s", e.__repr__()
                )

        await self.notify_worker_ready()

        lastev: str = "0"
        proced = 0
        while True:
            perf_start = time.perf_counter()
            try:
                newlastev, ingesterset = await self.get_new_assignments(lastev)
                if newlastev is None or ingesterset is None:
                    continue
                lastev = newlastev
            except RedisException:
                self._logger.warning("failed to access redis, exiting worker")
                break
            perf_got_assignments = time.perf_counter()

            if len(ingesterset) == 0:
                continue
            done = await self.poll_internals(ingesterset)
            if set(done) != {zmq.POLLIN}:
                self._logger.warning("not all sockets are pollIN %s", done)
                continue

            perf_got_work = time.perf_counter()

            event = await self.build_event(ingesterset)

            perf_assembled_event = time.perf_counter()
            self._logger.debug("received work %s", event)
            result = None
            if self.worker:
                try:
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(
                        None, self.worker.process_event, event, self.parameters
                    )
                except Exception as e:
                    self._logger.error(
                        "custom worker failed: %s\n%s",
                        e.__repr__(),
                        traceback.format_exc(),
                    )
            perf_custom_code = time.perf_counter()
            self._logger.debug("got result %s", result)
            rd = ResultData(
                event_number=event.event_number,
                worker=self.state.name,
                payload=result,
                parameters_hash=self.state.parameters_hash,
            )
            if self.out_socket:
                try:
                    header = rd.model_dump_json(exclude={"payload"}).encode("utf8")
                    body = pickle.dumps(rd.payload)
                    self._logger.debug(
                        "send result to reducer with header %s, len-payload %d",
                        header,
                        len(body),
                    )
                    await self.out_socket.send_multipart([header, body])
                except Exception as e:
                    self._logger.error("could not dump result %s", e.__repr__())
            perf_sent_result = time.perf_counter()
            proced += 1
            self.state.processed_events += 1
            if proced % 500 == 0:
                self._logger.info("processed %d events", proced)
            times = WorkerTimes.from_timestamps(
                perf_start,
                perf_got_assignments,
                perf_got_work,
                perf_assembled_event,
                perf_custom_code,
                perf_sent_result,
            )
            wu = WorkerUpdate(
                state=DistributedStateEnum.IDLE,
                completed=event.event_number,
                worker=self.state.name,
                processing_times=times,
            )
            self._logger.debug("all work done, notify controller with %s", wu)
            await self.redis.xadd(
                RedisKeys.ready(self.state.mapping_uuid),
                {"data": wu.model_dump_json()},
            )
        self._logger.info("work thread finished")

    async def finish_work(self) -> None:
        self._logger.info("finishing work")
        if self.worker:
            if hasattr(self.worker, "finish"):
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, self.worker.finish, self.parameters
                    )
                except Exception as e:
                    self._logger.error(
                        "custom worker finish failed: %s\n%s",
                        e.__repr__(),
                        traceback.format_exc(),
                    )

    async def restart_work(self, new_uuid: UUID4) -> None:
        self._logger.info("resetting config %s", new_uuid)
        if self.poll_task:
            self.poll_task.cancel()
            self.poll_task = None
            self._logger.debug("cancelled poll task")
        self.work_task.cancel()
        self._logger.info("clean up in sockets")
        for iname, ing in self._ingesters.items():
            while True:
                res = await ing.socket.poll(timeout=0.001)
                if res == zmq.POLLIN:
                    await ing.socket.recv_multipart(copy=False)
                    self._logger.debug("discarded internal message from %s", iname)
                else:
                    break

        self.state.mapping_uuid = new_uuid
        self.work_task = asyncio.create_task(self.work())
        self.work_task.add_done_callback(done_callback)

    async def manage_receiver(self) -> None:
        while True:
            config = await self.redis.get(RedisKeys.config("reducer"))
            if config is None:
                self._logger.warning("cannot get reducer configuration")
                await asyncio.sleep(1)
                continue
            cfg = ReducerState.model_validate_json(config)
            if cfg.service_uuid != self._reducer_service_uuid:
                # connect to a new reducer
                if self.out_socket is not None:
                    self.out_socket.close()
                self.out_socket = self.ctx.socket(zmq.PUSH)
                self.out_socket.connect(str(cfg.url))
                self._reducer_service_uuid = cfg.service_uuid
                self._logger.info("connected out_socket to reducer at %s", cfg.url)
            await asyncio.sleep(10)

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
                    and self._ingesters[iname].config.service_uuid != cfg.service_uuid
                ):
                    self._logger.warning(
                        "service_uuid of ingester changed from %s to %s, disconnecting",
                        self._ingesters[iname].config.service_uuid,
                        cfg.service_uuid,
                    )
                    self._ingesters[iname].socket.close()
                    del self._ingesters[iname]

                pinged_for_long = time.time() - self._ingesters[iname].pinged_since > 10
                reached_ingester = (
                    self.state.service_uuid in cfg.connected_workers.keys()
                )
                if (
                    iname in self._ingesters
                    and pinged_for_long
                    and not reached_ingester
                ):
                    self._logger.warning(
                        "we send pings, but don't reach ingester %s, disconnecting",
                        iname,
                    )
                    self._ingesters[iname].socket.close()
                    del self._ingesters[iname]

                if iname not in self._ingesters:
                    self._logger.info("adding new ingester %s", iname)
                    sock = self.ctx.socket(zmq.DEALER)
                    sock.setsockopt(zmq.IDENTITY, self.state.name.encode("ascii"))
                    sock.connect(str(cfg.url))
                    await sock.send(self.state.service_uuid.bytes)
                    self._ingesters[iname] = ConnectedIngester(config=cfg, socket=sock)

                await self._ingesters[iname].socket.send(self.state.service_uuid.bytes)
                self._logger.debug("pinged %s", iname)
            for iname in set(self._ingesters.keys()) - set(processed):
                self._logger.info("removing stale ingester %s", iname)
                self._ingesters[iname].socket.close()
                del self._ingesters[iname]
            self._stream_map = {
                s: conn_ing.socket
                for ing, conn_ing in self._ingesters.items()
                for s in conn_ing.config.streams
            }
            new_ingesters = [a.config for a in self._ingesters.values()]
            if self.state.ingesters != new_ingesters:
                self.state.ingesters = new_ingesters
                self._logger.info(
                    "changed ingester config, fast publish %s", new_ingesters
                )
                await self.publish_config()

            await asyncio.sleep(2)

    async def close(self) -> None:
        self.manage_ingester_task.cancel()
        self.manage_receiver_task.cancel()
        self.metrics_task.cancel()
        await self.redis.delete(RedisKeys.config("worker", self.state.name))
        await super().close()
        self.ctx.destroy()
        self._logger.info("worker closed")
