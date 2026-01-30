"""
This is the central service to orchestrate all distributed components

"""
import asyncio
import json
import os.path
from asyncio import Task
from collections import defaultdict
from types import UnionType
from typing import Any, AsyncGenerator, Optional, Annotated, Literal, AsyncIterator

import logging
import time

import cbor2
from pydantic import UUID4, ValidationError
from starlette.requests import Request
from starlette.responses import Response, FileResponse, StreamingResponse

from dranspose.distributed import DistributedSettings
from dranspose.helpers.utils import parameters_hash, done_callback, cancel_and_wait
from dranspose.mapping import MappingSequence, Map
import redis.asyncio as redis
import redis.exceptions as rexceptions
from importlib.metadata import version

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware

from dranspose.parameters import (
    Parameter,
    ParameterType,
    StrParameter,
)
from dranspose.protocol import (
    IngesterState,
    WorkerState,
    RedisKeys,
    EnsembleState,
    WorkerUpdate,
    DistributedStateEnum,
    WorkerName,
    StreamName,
    EventNumber,
    VirtualWorker,
    WorkParameter,
    ControllerUpdate,
    ReducerState,
    WorkerTimes,
    HashDigest,
    ParameterName,
    SystemLoadType,
    IntervalLoad,
    WorkerLoad,
    DistributedUpdate,
    ReducerUpdate,
    IngesterUpdate,
    WorkAssignmentList,
    WorkAssignment,
    IngesterName,
    MappingName,
)

logger = logging.getLogger(__name__)


class ControllerSettings(DistributedSettings):
    pass


class Controller:
    def __init__(self, settings: ControllerSettings | None = None):
        self.settings = settings
        if self.settings is None:
            self.settings = ControllerSettings()

        self.redis = redis.from_url(
            f"{self.settings.redis_dsn}?decode_responses=True&protocol=3"
        )
        self.mapping = MappingSequence(parts={}, sequence=[])
        self.mapping_update_lock = asyncio.Lock()
        self.parameters: dict[ParameterName, WorkParameter] = {}
        self.parameters_hash = parameters_hash(self.parameters)
        self.completed: dict[EventNumber, list[WorkerName]] = defaultdict(list)
        self.to_reduce: set[tuple[EventNumber, WorkerName]] = set()
        self.reduced: dict[EventNumber, list[WorkerName]] = defaultdict(list)
        self.processed_event_no: int = 0
        self.completed_events: list[int] = []
        self.finished_components: list[UnionType] = []
        self.external_stop = False
        self.assign_task: Task[None]
        self.config_fetch_time: float = 0
        self.config_cache: EnsembleState | None = None
        self.default_task: Task[None]
        self.consistent_task: Task[None]
        self.worker_timing: dict[
            WorkerName, dict[EventNumber, WorkerTimes]
        ] = defaultdict(dict)
        self.start_time: float

    async def run(self) -> None:
        logger.debug("started controller run")
        dist_lock = await self.redis.set(
            RedisKeys.lock(),
            "🔒",
            ex=10,
            nx=True,
        )
        logger.debug("result of lock acquisition %s", dist_lock)
        timeout = 30
        while dist_lock is None:
            logger.warning("another controller is already running, will retry ")
            await asyncio.sleep(2)
            timeout -= 1
            dist_lock = await self.redis.set(
                RedisKeys.lock(),
                "🔒",
                ex=10,
                nx=True,
            )
            if timeout < 0:
                logger.error("could not acquire lock!")
                raise RuntimeError("Could not acquire lock")
        logger.info("controller lock acquired")

        self.assign_task = asyncio.create_task(self.assign_work())
        self.assign_task.add_done_callback(done_callback)
        self.default_task = asyncio.create_task(self.default_parameters())
        self.default_task.add_done_callback(done_callback)
        self.consistent_task = asyncio.create_task(self.consistent_parameters())
        self.consistent_task.add_done_callback(done_callback)
        self.lock_task = asyncio.create_task(self.hold_lock())
        self.lock_task.add_done_callback(done_callback)

    async def hold_lock(self) -> None:
        while True:
            await asyncio.sleep(7)
            dist_lock = await self.redis.set(
                RedisKeys.lock(),
                "🔒",
                ex=10,
                xx=True,
            )
            if dist_lock is False:
                logger.warning("The lock was lost")

    async def get_configs(self) -> EnsembleState:
        if time.time() - self.config_fetch_time < 0.5 and self.config_cache is not None:
            return self.config_cache
        async with self.redis.pipeline() as pipe:
            await pipe.keys(RedisKeys.config("ingester"))
            await pipe.keys(RedisKeys.config("worker"))
            ingester_keys, worker_keys = await pipe.execute()
        async with self.redis.pipeline() as pipe:
            await pipe.mget(ingester_keys)
            await pipe.mget(worker_keys)
            await pipe.get(RedisKeys.config("reducer"))
            ingester_json, worker_json, reducer_json = await pipe.execute()

        ingesters = []
        for i in ingester_json:
            try:
                ingester = IngesterState.model_validate_json(i)
                ingesters.append(ingester)
            except ValidationError:
                pass
        workers = []
        for w in worker_json:
            try:
                worker = WorkerState.model_validate_json(w)
                workers.append(worker)
            except ValidationError:
                pass
        reducer = None
        if reducer_json:
            reducer = ReducerState.model_validate_json(reducer_json)
        dranspose_version = version("dranspose")

        parameters_version: dict[ParameterName, UUID4] = {
            n: p.uuid for n, p in self.parameters.items()
        }
        redis_param_keys = await self.redis.keys(RedisKeys.parameters("*", "*"))
        logger.debug("redis param keys are %s", redis_param_keys)
        self.config_cache = EnsembleState(
            ingesters=ingesters,
            workers=workers,
            reducer=reducer,
            controller_version=dranspose_version,
            parameters_version=parameters_version,
            parameters_hash=self.parameters_hash,
        )
        self.config_fetch_time = time.time()
        return self.config_cache

    async def get_load(self, intervals: list[int], scan: bool = True) -> SystemLoadType:
        ret = {}
        for wn, wt in self.worker_timing.items():
            last_event = max(wt.keys())
            itval: dict[int | Literal["scan"], IntervalLoad] = {}
            for interval in intervals:
                evs = list(
                    filter(
                        lambda x: x >= len(self.completed_events) - interval,
                        wt.keys(),
                    )
                )
                itval[interval] = IntervalLoad(
                    total=sum([wt[e].total for e in evs]),
                    active=sum([wt[e].active for e in evs]),
                    events=len(evs),
                )
            if scan:
                evs = list(wt.keys())
                itval["scan"] = IntervalLoad(
                    total=sum([wt[e].total for e in evs]),
                    active=sum([wt[e].active for e in evs]),
                    events=len(evs),
                )
            ret[wn] = WorkerLoad(last_event=last_event, intervals=itval)
        return ret

    async def dump_map_and_parameters(self) -> None:
        """Saves all current parameters to a JSON/bin file if dump_prefix is defined."""
        dump_prefix = self.parameters[ParameterName("dump_prefix")].data.decode("utf8")
        if len(dump_prefix) > 0:
            parts = {
                str(n): m.model_dump(mode="json") for n, m in self.mapping.parts.items()
            }
            mapping_json = {
                "parts": parts,
                "sequence": self.mapping.sequence,
            }
            filename = f"{dump_prefix}mapping-{self.mapping.uuid}.json"
            try:
                with open(filename, "w") as f:
                    json.dump(mapping_json, f)
                logger.info(f"Mapping saved to {filename}")
            except Exception as e:
                logger.error(f"Could not write mapping dump {e}")
            filename = f"parameters-{self.mapping.uuid}"
            bin_file = False
            pars = []
            for name, par in self.parameters.items():
                if b"\x00" in par.data:
                    bin_file = True
                    pars.append({"name": name, "data": par.data})
                else:
                    pars.append({"name": name, "data": par.data.decode("utf8")})
            try:
                if bin_file:
                    filename = f"{dump_prefix}{filename}.cbor"
                    with open(filename, "wb") as f:
                        cbor2.dump(pars, f)
                else:
                    filename = f"{dump_prefix}{filename}.json"
                    with open(filename, "w") as f:
                        json.dump(pars, f)
                logger.info(f"Parameters saved to {filename}")
            except Exception as e:
                logger.error(f"Could not write parameters dump {e}")

    async def set_mapping(self, m: MappingSequence) -> None:
        async with self.mapping_update_lock:
            logger.debug("cancelling assign task")
            await cancel_and_wait(self.assign_task)
            logger.debug(
                "deleting keys %s and %s",
                RedisKeys.ready(self.mapping.uuid),
                RedisKeys.assigned(self.mapping.uuid),
            )
            await self.redis.delete(RedisKeys.ready(self.mapping.uuid))
            await self.redis.delete(RedisKeys.assigned(self.mapping.uuid))

            # cleaned up
            self.mapping = m
            cupd = ControllerUpdate(
                mapping_uuid=self.mapping.uuid,
                parameters_version={n: p.uuid for n, p in self.parameters.items()},
                active_streams=list(m.all_streams),
            )
            logger.debug("send controller update %s", cupd)
            await self.redis.xadd(
                RedisKeys.updates(),
                {"data": cupd.model_dump_json()},
            )

            cfgs = await self.get_configs()
            timeout = 0
            while cfgs.reducer is None or set(
                [u.mapping_uuid for u in cfgs.ingesters]
                + [u.mapping_uuid for u in cfgs.workers]
                + [cfgs.reducer.mapping_uuid]
            ) != {self.mapping.uuid}:
                await asyncio.sleep(0.1)
                timeout += 1
                cfgs = await self.get_configs()
                if timeout < 50:
                    logger.debug("updated configs %s", cfgs)
                elif timeout % 10 == 0:
                    logger.warning(
                        "still waiting for configs %s to reach everyone", cfgs
                    )
                elif timeout > 100:
                    logger.error("could not distribute mapping within 10 seconds")
                    raise TimeoutError("could not distribute mapping")
            logger.info("new mapping with uuid %s distributed", self.mapping.uuid)
            if ParameterName("dump_prefix") in self.parameters:
                await self.dump_map_and_parameters()
            self.assign_task = asyncio.create_task(self.assign_work())
            self.assign_task.add_done_callback(done_callback)

    async def set_param(self, name: ParameterName, data: bytes) -> HashDigest:
        """Writes a parameter in the controller's store, updates the controller's
        hash, and updating the parameter hash. It also sends a controller update
        to the Redis stream to notify other components of the new parameter."""

        param = WorkParameter(name=name, data=data)
        logger.debug("distributing parameter %s with uuid %s", param.name, param.uuid)
        await self.redis.set(RedisKeys.parameters(name, param.uuid), param.data)
        self.parameters[name] = param
        logger.debug("stored parameter %s locally", name)
        self.parameters_hash = parameters_hash(self.parameters)
        logger.debug("parameter hash is now %s", self.parameters_hash)
        cupd = ControllerUpdate(
            mapping_uuid=self.mapping.uuid,
            parameters_version={n: p.uuid for n, p in self.parameters.items()},
            target_parameters_hash=self.parameters_hash,
        )
        logger.debug("send update %s", cupd)
        await self.redis.xadd(
            RedisKeys.updates(),
            {"data": cupd.model_dump_json()},
        )
        return self.parameters_hash

    async def describe_parameters(self) -> list[ParameterType]:
        desc_keys = await self.redis.keys(RedisKeys.parameter_description())
        param_json = await self.redis.mget(desc_keys)

        params: list[ParameterType] = []
        for i in param_json:
            val: ParameterType = Parameter.validate_json(i)
            params.append(val)

        params.append(
            StrParameter(
                name=ParameterName("dump_prefix"),
                description="Prefix to dump ingester values",
            )
        )
        return sorted(params, key=lambda x: x.name)

    async def default_parameters(self) -> None:
        """The descriptions (and default values) for the parameters are defined
        in the worker's payload and published to redis. default_parameters
        retrieves the description and adds it to the list of parameters if
        they are missing."""
        while True:
            try:
                desc_keys = await self.redis.keys(RedisKeys.parameter_description())
                param_json = await self.redis.mget(desc_keys)
                for i in param_json:
                    if i is None:
                        # this is a race condition where the key gets deleted between keys and mget
                        continue
                    val: ParameterType = Parameter.validate_json(i)
                    if val.name not in self.parameters:
                        logger.info(
                            "set parameter %s to default %s, (type %s)",
                            val.name,
                            val.default,
                            val.__class__,
                        )
                        await self.set_param(
                            val.name, val.__class__.to_bytes(val.default)
                        )
                await asyncio.sleep(2)
            except asyncio.exceptions.CancelledError:
                break

    async def consistent_parameters(self) -> None:
        while True:
            try:
                # make sure self.parameters is present in redis

                key_names = []
                for name, param in self.parameters.items():
                    key_names.append(RedisKeys.parameters(name, param.uuid))
                if len(key_names) > 0:
                    ex_key_no = await self.redis.exists(*key_names)
                    logger.debug(
                        "check for param values in redis, %d exist of %d: %s",
                        ex_key_no,
                        len(key_names),
                        key_names,
                    )
                    if ex_key_no < len(key_names):
                        logger.warning(
                            "the redis parameters don't match the controller parameters, rewriting"
                        )
                        async with self.redis.pipeline() as pipe:
                            for name, param in self.parameters.items():
                                await pipe.set(
                                    RedisKeys.parameters(name, param.uuid), param.data
                                )
                            await pipe.execute()

                consistent = []
                cfg = await self.get_configs()
                if cfg.reducer and cfg.reducer.parameters_hash != self.parameters_hash:
                    consistent.append(("reducer", cfg.reducer.parameters_hash))
                for wo in cfg.workers:
                    if wo.parameters_hash != self.parameters_hash:
                        consistent.append((wo.name, wo.parameters_hash))
                for ing in cfg.ingesters:
                    if ing.parameters_hash != self.parameters_hash:
                        consistent.append((ing.name, ing.parameters_hash))

                if len(consistent) > 0:
                    logger.info(
                        "inconsistent parameters %s, redistribute hash %s",
                        consistent,
                        self.parameters_hash,
                    )
                    cupd = ControllerUpdate(
                        mapping_uuid=self.mapping.uuid,
                        parameters_version={
                            n: p.uuid for n, p in self.parameters.items()
                        },
                        target_parameters_hash=self.parameters_hash,
                    )
                    logger.debug("send consistency update %s", cupd)
                    await self.redis.xadd(
                        RedisKeys.updates(),
                        {"data": cupd.model_dump_json()},
                    )
                await asyncio.sleep(2)
            except asyncio.exceptions.CancelledError:
                break

    async def _update_processing_times(self, update: WorkerUpdate) -> None:
        if update.completed is None:
            return
        if update.processing_times:
            self.worker_timing[update.worker][
                update.completed[-1]
            ] = update.processing_times
        for compev, has_result in zip(update.completed, update.has_result):
            self.completed[compev].append(update.worker)
            logger.debug("added completed to set %s", self.completed)
            wa = self.mapping.get_event_workers(compev)
            if wa.get_all_workers() == set(self.completed[compev]):
                self.completed_events.append(compev)
            if has_result:
                toadd = True
                if compev in self.reduced:
                    logger.debug(
                        "events %s already received from reducer", self.reduced
                    )
                    if update.worker in self.reduced[compev]:
                        # process worker update very late and already got reduced
                        logger.debug(
                            "event %s from worker %s was already reduced",
                            compev,
                            update.worker,
                        )
                        toadd = False
                if toadd:
                    logger.debug(
                        "waiting for reduction for event %s from worker %s",
                        compev,
                        update.worker,
                    )
                    self.to_reduce.add((compev, update.worker))
        # logger.error("time processtime %s", time.perf_counter() - start)

    async def assign_worker_in_mapping(
        self, worker: WorkerName, completed: EventNumber
    ) -> None:
        cfg = await self.get_configs()
        # logger.error("time cfg %s", time.perf_counter() - start)
        logger.debug(
            "assigning worker %s with all %s",
            worker,
            [ws.name for ws in cfg.workers],
        )
        virt = self.mapping.assign_next(
            next(w for w in cfg.workers if w.name == worker),
            cfg.workers,
            completed=completed,
            horizon=5,
        )
        # logger.error("time assign %s", time.perf_counter() - start)
        logger.debug("assigned worker %s to %s", worker, virt)
        logger.debug(
            "send out complete events in range(%d, %d)",
            self.processed_event_no,
            self.mapping.complete_events,
        )
        assignments: list[WorkAssignment] = []
        for evn in range(self.processed_event_no, self.mapping.complete_events):
            wrks = self.mapping.get_event_workers(EventNumber(evn))
            assignments.append(wrks)
            if wrks.get_all_workers() == set():
                self.completed[wrks.event_number] = []
                self.completed_events.append(wrks.event_number)
            if evn % 1000 == 0:
                logger.info(
                    "1000 events in %lf",
                    time.perf_counter() - self.start_time,
                )
                self.start_time = time.perf_counter()
        if len(assignments) > 0:
            logger.debug("send out assignment %s", assignments)
            await self.redis.xadd(
                RedisKeys.assigned(self.mapping.uuid),
                {"data": WorkAssignmentList.dump_json(assignments)},
            )

        # logger.error("time sent out %s", time.perf_counter() - start)
        self.processed_event_no = self.mapping.complete_events

    async def _process_worker_update(self, update: WorkerUpdate) -> None:
        logger.debug("got a ready worker %s", update)
        if update.state == DistributedStateEnum.READY:
            await self.assign_worker_in_mapping(update.worker, EventNumber(0))
        if update.state == DistributedStateEnum.IDLE:
            # start = time.perf_counter()
            await self.assign_worker_in_mapping(update.worker, update.completed[-1])

            asyncio.create_task(self._update_processing_times(update))

    async def completed_finish(self) -> bool:
        fin_workers: set[WorkerName] = set()
        reducer = False
        fin_ingesters: set[IngesterName] = set()
        for upd in self.finished_components:
            if isinstance(upd, WorkerUpdate):
                fin_workers.add(upd.worker)
            elif isinstance(upd, ReducerUpdate):
                reducer = True
            elif isinstance(upd, IngesterUpdate):
                fin_ingesters.add(upd.ingester)

        cfgs = await self.get_configs()

        return (
            set([w.name for w in cfgs.workers]) == fin_workers
            and set([i.name for i in cfgs.ingesters]) == fin_ingesters
            and reducer
        )

    async def assign_work(self) -> None:
        last = 0
        self.external_stop = False
        self.processed_event_no = 0
        self.completed = defaultdict(list)
        self.reduced = defaultdict(list)
        self.completed_events = []
        self.to_reduce = set()
        self.finished_components = []
        self.worker_timing = defaultdict(dict)
        notify_finish = True
        self.start_time = time.perf_counter()
        while True:
            try:
                workers = await self.redis.xread(
                    {RedisKeys.ready(self.mapping.uuid): last},
                    block=1000,
                )
                if RedisKeys.ready(self.mapping.uuid) in workers:
                    for ready in workers[RedisKeys.ready(self.mapping.uuid)][0]:
                        logger.debug("got ready raw data: %s", ready)
                        update = DistributedUpdate.validate_json(ready[1]["data"])
                        if isinstance(update, WorkerUpdate):
                            await self._process_worker_update(update)
                        elif isinstance(update, ReducerUpdate):
                            if (
                                update.completed is not None
                                and update.worker is not None
                            ):
                                compev = update.completed
                                self.reduced[compev].append(update.worker)
                                logger.debug("added reduced to set %s", self.reduced)
                                if (compev, update.worker) in self.to_reduce:
                                    self.to_reduce.remove((compev, update.worker))
                        elif isinstance(update, IngesterUpdate):
                            pass

                        if hasattr(update, "state"):
                            if update.state == DistributedStateEnum.FINISHED:
                                logger.info("distributed %s has finished", update)
                                self.finished_components.append(update)

                        last = ready[0]
                logger.debug(
                    "checking if finished, completed %d, len %d, to_reduce %s",
                    len(self.completed_events),
                    self.mapping.len(),
                    self.to_reduce,
                )
                if (
                    (
                        len(self.completed_events) > 0
                        and len(self.completed_events) == self.mapping.len()
                        and len(self.to_reduce) == 0
                    )
                    or self.external_stop
                ) and notify_finish:
                    # all events done, send close
                    cupd = ControllerUpdate(
                        mapping_uuid=self.mapping.uuid,
                        parameters_version={
                            n: p.uuid for n, p in self.parameters.items()
                        },
                        finished=True,
                    )
                    logger.debug("send finished update %s", cupd)
                    await self.redis.xadd(
                        RedisKeys.updates(),
                        {"data": cupd.model_dump_json()},
                    )
                    notify_finish = False
            except rexceptions.ConnectionError:
                break

    async def close(self) -> None:
        await cancel_and_wait(self.default_task)
        await cancel_and_wait(self.consistent_task)
        await self.redis.delete(RedisKeys.updates())
        logger.info("deleted updates redis stream")
        queues = await self.redis.keys(RedisKeys.ready("*"))
        if len(queues) > 0:
            await self.redis.delete(*queues)
            logger.info("deleted ready queues %s", queues)
        assigned = await self.redis.keys(RedisKeys.assigned("*"))
        if len(assigned) > 0:
            await self.redis.delete(*assigned)
        params = await self.redis.keys(RedisKeys.parameters("*", "*"))
        if len(params) > 0:
            await self.redis.delete(*params)
            logger.info("deleted parameters %s", params)
        param_descr = await self.redis.keys(RedisKeys.parameter_description("*"))
        if len(param_descr) > 0:
            await self.redis.delete(*param_descr)
            logger.info("deleted parameter descriptions %s", params)
        await cancel_and_wait(self.lock_task)
        await self.redis.delete(RedisKeys.lock())
        await self.redis.aclose()
        logger.info("controller closed")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    app.state.ctrl = Controller()
    # run_task = asyncio.create_task(ctrl.run())
    await app.state.ctrl.run()
    # run_task.add_done_callback(done_callback)
    yield
    # await cancel_and_wait(run_task)
    await app.state.ctrl.close()
    # Clean up the ML models and release the resources


def routes_legacy(app: FastAPI) -> None:
    @app.post("/api/v1/mapping", deprecated=True)
    async def set_mapping(
        request: Request,
        mapping: dict[StreamName, list[Optional[list[VirtualWorker]]]],
        all_wrap: bool = True,
    ) -> UUID4 | str:
        ctrl = request.app.state.ctrl
        config = await ctrl.get_configs()
        if set(mapping.keys()) - set(config.get_streams()) != set():
            logger.warning(
                "bad request streams %s not available",
                set(mapping.keys()) - set(config.get_streams()),
            )
            raise HTTPException(
                status_code=400,
                detail=f"streams {set(mapping.keys()) - set(config.get_streams())} not available",
            )
        m = MappingSequence(
            parts={MappingName("main"): mapping},
            sequence=[MappingName("main")],
            add_start_end=all_wrap,
        )
        if len(config.workers) < m.min_workers():
            logger.warning(
                "only %d workers available, but %d required",
                len(config.workers),
                m.min_workers(),
            )
            raise HTTPException(
                status_code=400,
                detail=f"only {len(config.workers)} workers available, but {m.min_workers()} required",
            )
        await ctrl.set_mapping(m)
        return m.uuid

    @app.get("/api/v1/logs", deprecated=True)
    async def get_logs(request: Request, level: str = "INFO") -> Any:
        data = await request.app.state.ctrl.redis.xrange("dranspose_logs", "-", "+")
        logs = []
        # TODO: once python 3.10 support is dropped, use
        # levels = logging.getLevelNamesMapping()
        levels = {
            level: logging.__getattribute__(level)
            for level in dir(logging)
            if level.isupper() and isinstance(logging.__getattribute__(level), int)
        }
        minlevel = levels.get(level.upper(), logging.INFO)
        for entry in data:
            msglevel = levels[entry[1].get("levelname", "DEBUG")]
            if msglevel >= minlevel:
                logs.append(entry[1])
        return logs

    @app.post("/api/v1/sardana_hook", deprecated=True)
    async def set_sardana_hook(
        request: Request, info: dict[Literal["streams"] | Literal["scan"], Any]
    ) -> UUID4 | str:
        ctrl = request.app.state.ctrl
        config = await ctrl.get_configs()
        print(info)
        if "scan" not in info:
            return "no scan info"
        if "nb_points" not in info["scan"]:
            return "no nb_points in scan"
        if "streams" not in info:
            return "streams required"
        for st in set(config.get_streams()).intersection(set(info["streams"])):
            print("use stream", st)
        logger.debug("create new mapping")
        m = Map.from_uniform(
            set(config.get_streams()).intersection(set(info["streams"])),
            info["scan"]["nb_points"],
        )
        s = MappingSequence(
            parts={MappingName("sardana"): m.mapping},
            sequence=[MappingName("sardana")],
            add_start_end=True,
        )
        logger.debug("set mapping")
        await ctrl.set_mapping(s)
        return s.uuid


def routes_status(app: FastAPI) -> None:
    @app.get("/api/v1/config")
    async def get_configs(request: Request) -> EnsembleState:
        return await request.app.state.ctrl.get_configs()

    @app.get("/api/v1/status")
    async def get_status(request: Request) -> dict[str, Any]:
        ctrl = request.app.state.ctrl
        return {
            "work_completed": ctrl.completed,
            "last_assigned": ctrl.mapping.complete_events,
            "assignment": [am.assignments for am in ctrl.mapping.active_maps],
            "completed_events": ctrl.completed_events,
            "finished": await ctrl.completed_finish(),
            "processing_times": ctrl.worker_timing,
        }

    @app.get("/api/v1/load")
    async def get_load(
        request: Request,
        intervals: Annotated[list[int] | None, Query()] = None,
        scan: bool = True,
    ) -> SystemLoadType:
        if intervals is None:
            intervals = [1, 10]
        return await request.app.state.ctrl.get_load(intervals, scan)

    @app.get("/api/v1/progress")
    async def get_progress(request: Request) -> dict[str, Any]:
        ctrl = request.app.state.ctrl
        return {
            "last_assigned": ctrl.mapping.complete_events,
            "completed_events": len(ctrl.completed_events),
            "total_events": ctrl.mapping.len(),
            "finished": await ctrl.completed_finish(),
        }


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
    )

    @app.get("/")
    async def read_index() -> FileResponse:
        return FileResponse(
            os.path.join(os.path.dirname(__file__), "frontend", "index.html")
        )

    @app.get("/api/v1/mapping", deprecated=True)
    async def get_mapping(request: Request) -> dict[str, Any]:
        """alias for backwards compatibility"""
        return await get_sequence(request)

    @app.get("/api/v1/sequence")
    async def get_sequence(request: Request) -> dict[str, Any]:
        ctrl = request.app.state.ctrl
        return {"parts": ctrl.mapping.parts, "sequence": ctrl.mapping.sequence}

    complex_parts_example = {
        "parts": {
            "main": {
                "eiger": [
                    [{"tags": ["generic"], "constraint": 2}],
                    [{"tags": ["generic"], "constraint": 4}],
                    [{"tags": ["generic"], "constraint": 6}],
                    [{"tags": ["generic"], "constraint": 8}],
                ],
                "orca": [
                    [{"tags": ["generic"], "constraint": 3}],
                    [{"tags": ["generic"], "constraint": 5}],
                    [{"tags": ["generic"], "constraint": 7}],
                    [{"tags": ["generic"], "constraint": 9}],
                ],
                "alba": [
                    [
                        {"tags": ["generic"], "constraint": 2},
                        {"tags": ["generic"], "constraint": 3},
                    ],
                    [
                        {"tags": ["generic"], "constraint": 4},
                        {"tags": ["generic"], "constraint": 5},
                    ],
                    [
                        {"tags": ["generic"], "constraint": 6},
                        {"tags": ["generic"], "constraint": 7},
                    ],
                    [
                        {"tags": ["generic"], "constraint": 8},
                        {"tags": ["generic"], "constraint": 9},
                    ],
                ],
                "slow": [
                    None,
                    None,
                    None,
                    [
                        {"tags": ["generic"], "constraint": 8},
                        {"tags": ["generic"], "constraint": 9},
                    ],
                ],
            }
        },
        "sequence": ["main"],
    }
    simple_parts_example = {
        "main": {
            "orca": [
                [{"tags": ["generic"], "constraint": 1}],
                [{"tags": ["generic"], "constraint": 2}],
                [{"tags": ["generic"], "constraint": 3}],
            ],
            "eiger": [
                [{"tags": ["generic"], "constraint": 1}],
                [{"tags": ["generic"], "constraint": 2}],
                [{"tags": ["generic"], "constraint": 3}],
            ],
        }
    }

    @app.post("/api/v1/sequence")
    async def set_sequence(
        request: Request,
        parts: Annotated[
            dict[MappingName, dict[StreamName, list[Optional[list[VirtualWorker]]]]],
            Body(examples=[complex_parts_example, simple_parts_example]),
        ],
        sequence: Annotated[list[MappingName], Body(examples=[["main", "main"]])],
        all_wrap: bool = True,
    ) -> UUID4 | str:
        ctrl = request.app.state.ctrl
        config = await ctrl.get_configs()
        m = MappingSequence(
            parts=parts,
            sequence=sequence,
            add_start_end=all_wrap,
        )
        if m.all_streams - set(config.get_streams()) != set():
            logger.warning(
                "bad request streams %s not available",
                m.all_streams - set(config.get_streams()),
            )
            raise HTTPException(
                status_code=400,
                detail=f"streams {m.all_streams - set(config.get_streams())} not available",
            )

        if len(config.workers) < m.min_workers():
            logger.warning(
                "only %d workers available, but %d required",
                len(config.workers),
                m.min_workers(),
            )
            raise HTTPException(
                status_code=400,
                detail=f"only {len(config.workers)} workers available, but {m.min_workers()} required",
            )
        await ctrl.set_mapping(m)
        return m.uuid

    @app.post("/api/v1/stop")
    async def stop(request: Request) -> None:
        logger.info("externally stopped scan")
        request.app.state.ctrl.external_stop = True

    async def log_streamer(ctrl: Controller) -> AsyncIterator[str]:
        latest = await ctrl.redis.xrevrange("dranspose_logs", count=1)
        last = 0
        if len(latest) > 0:
            last = latest[0][0]

        while True:
            update_msgs = await ctrl.redis.xread({"dranspose_logs": last}, block=1300)
            if "dranspose_logs" in update_msgs:
                update_msg = update_msgs["dranspose_logs"][0][-1]
                last = update_msg[0]
                for log in update_msgs["dranspose_logs"][0]:
                    yield json.dumps(log[1]) + "\n"

    @app.get("/api/v1/log_stream")
    async def get_log_stream(request: Request) -> StreamingResponse:
        return StreamingResponse(log_streamer(request.app.state.ctrl))

    @app.post("/api/v1/parameter/{name}")
    async def post_param(request: Request, name: ParameterName) -> HashDigest:
        data = await request.body()
        logger.info("got %s: %s (len %d)", name, data[:100], len(data))
        u = await request.app.state.ctrl.set_param(name, data)
        return u

    @app.get("/api/v1/parameter/{name}")
    async def get_param(request: Request, name: ParameterName) -> Response:
        ctrl = request.app.state.ctrl
        if name not in ctrl.parameters:
            raise HTTPException(status_code=404, detail="Parameter not found")

        data = ctrl.parameters[name].data
        return Response(data, media_type="application/x.bytes")

    @app.get("/api/v1/parameters")
    async def param_descr(request: Request) -> list[ParameterType]:
        return await request.app.state.ctrl.describe_parameters()

    routes_status(app)
    routes_legacy(app)

    return app


app = create_app()
