"""
This is the central service to orchestrate all distributed components

"""
import asyncio
from asyncio import Task
from collections import defaultdict
from typing import Dict, List, Any, AsyncGenerator, Optional, Annotated, Literal

import logging
import time

from pydantic import UUID4
from starlette.requests import Request
from starlette.responses import Response

from dranspose.distributed import DistributedSettings
from dranspose.helpers.utils import parameters_hash, done_callback
from dranspose.mapping import Mapping
import redis.asyncio as redis
import redis.exceptions as rexceptions

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query

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
    Digest,
    ParameterName,
    SystemLoadType,
    IntervalLoad,
    WorkerLoad,
    DistributedUpdate,
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
        self.mapping = Mapping({})
        self.mapping_update_lock = asyncio.Lock()
        self.parameters: dict[ParameterName, WorkParameter] = {}
        self.parameters_hash = parameters_hash(self.parameters)
        self.completed: dict[EventNumber, list[WorkerName]] = defaultdict(list)
        self.completed_events: list[int] = []
        self.assign_task: Task[None]
        self.worker_timing: dict[
            WorkerName, dict[EventNumber, WorkerTimes]
        ] = defaultdict(dict)

    async def run(self) -> None:
        logger.debug("started controller run")
        self.assign_task = asyncio.create_task(self.assign_work())
        self.assign_task.add_done_callback(done_callback)

    async def get_configs(self) -> EnsembleState:
        async with self.redis.pipeline() as pipe:
            await pipe.keys(RedisKeys.config("ingester"))
            await pipe.keys(RedisKeys.config("worker"))
            ingester_keys, worker_keys = await pipe.execute()
        async with self.redis.pipeline() as pipe:
            await pipe.mget(ingester_keys)
            await pipe.mget(worker_keys)
            await pipe.get(RedisKeys.config("reducer"))
            ingester_json, worker_json, reducer_json = await pipe.execute()

        ingesters = [IngesterState.model_validate_json(i) for i in ingester_json]
        workers = [WorkerState.model_validate_json(w) for w in worker_json]
        reducer = None
        if reducer_json:
            reducer = ReducerState.model_validate_json(reducer_json)
        return EnsembleState(ingesters=ingesters, workers=workers, reducer=reducer)

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

    async def set_mapping(self, m: Mapping) -> None:
        async with self.mapping_update_lock:
            logger.debug("cancelling assign task")
            self.assign_task.cancel()
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
            )
            logger.debug("send controller update %s", cupd)
            await self.redis.xadd(
                RedisKeys.updates(),
                {"data": cupd.model_dump_json()},
            )

            cfgs = await self.get_configs()
            while cfgs.reducer is None or set(
                [u.mapping_uuid for u in cfgs.ingesters]
                + [u.mapping_uuid for u in cfgs.workers]
                + [cfgs.reducer.mapping_uuid]
            ) != {self.mapping.uuid}:
                await asyncio.sleep(0.1)
                cfgs = await self.get_configs()
                logger.debug("updated configs %s", cfgs)
            logger.info("new mapping with uuid %s distributed", self.mapping.uuid)
            self.assign_task = asyncio.create_task(self.assign_work())
            self.assign_task.add_done_callback(done_callback)

    async def set_param(self, name: ParameterName, data: bytes) -> Digest:
        param = WorkParameter(name=name, data=data)
        logger.debug("distributing parameter %s with uuid %s", param.name, param.uuid)
        await self.redis.set(RedisKeys.parameters(name, param.uuid), param.data)
        self.parameters[name] = param
        logger.debug("stored parameters")
        self.parameters_hash = parameters_hash(self.parameters)
        cupd = ControllerUpdate(
            mapping_uuid=self.mapping.uuid,
            parameters_version={n: p.uuid for n, p in self.parameters.items()},
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
            val: ParameterType = Parameter.validate_json(i)  # type: ignore
            params.append(val)

        params.append(
            StrParameter(
                name="dump_prefix", description="Prefix to dump ingester values"
            )
        )

        return sorted(params, key=lambda x: x.name)

    async def assign_work(self) -> None:
        last = 0
        event_no = 0
        self.completed = defaultdict(list)
        self.completed_events = []
        self.worker_timing = defaultdict(dict)
        notify_finish = True
        start = time.perf_counter()
        while True:
            try:
                workers = await self.redis.xread(
                    {RedisKeys.ready(self.mapping.uuid): last},
                    block=1000,
                )
                if RedisKeys.ready(self.mapping.uuid) in workers:
                    for ready in workers[RedisKeys.ready(self.mapping.uuid)][0]:
                        update = DistributedUpdate.model_validate_json(ready[1]["data"])
                        if isinstance(update, WorkerUpdate):
                            logger.debug("got a ready worker %s", update)
                            if update.state == DistributedStateEnum.IDLE:
                                cfg = await self.get_configs()
                                logger.debug(
                                    "assigning worker %s with all %s",
                                    update.worker,
                                    [ws.name for ws in cfg.workers],
                                )
                                virt = self.mapping.assign_next(
                                    next(
                                        w
                                        for w in cfg.workers
                                        if w.name == update.worker
                                    ),
                                    cfg.workers,
                                )
                                if not update.new:
                                    if update.processing_times:
                                        self.worker_timing[update.worker][
                                            update.completed
                                        ] = update.processing_times
                                    compev = update.completed
                                    self.completed[compev].append(update.worker)
                                    logger.debug(
                                        "added completed to set %s", self.completed
                                    )
                                    wa = self.mapping.get_event_workers(compev)
                                    if wa.get_all_workers() == set(
                                        self.completed[compev]
                                    ):
                                        self.completed_events.append(compev)
                                logger.debug(
                                    "assigned worker %s to %s", update.worker, virt
                                )
                                async with self.redis.pipeline() as pipe:
                                    logger.debug(
                                        "send out complete events in range(%d, %d)",
                                        event_no,
                                        self.mapping.complete_events,
                                    )
                                    for evn in range(
                                        event_no, self.mapping.complete_events
                                    ):
                                        wrks = self.mapping.get_event_workers(
                                            EventNumber(evn)
                                        )
                                        await pipe.xadd(
                                            RedisKeys.assigned(self.mapping.uuid),
                                            {"data": wrks.model_dump_json()},
                                            id=evn + 1,
                                        )
                                        if wrks.get_all_workers() == set():
                                            self.completed[wrks.event_number] = []
                                            self.completed_events.append(
                                                wrks.event_number
                                            )
                                        if evn % 1000 == 0:
                                            logger.info(
                                                "1000 events in %lf",
                                                time.perf_counter() - start,
                                            )
                                            start = time.perf_counter()
                                    await pipe.execute()
                                event_no = self.mapping.complete_events
                            last = ready[0]
                logger.debug(
                    "checking if finished, completed %d, len %d",
                    len(self.completed_events),
                    self.mapping.len(),
                )
                if (
                    len(self.completed_events) > 0
                    and len(self.completed_events) == self.mapping.len()
                    and notify_finish
                ):
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
        await self.redis.delete(RedisKeys.updates())
        queues = await self.redis.keys(RedisKeys.ready("*"))
        if len(queues) > 0:
            await self.redis.delete(*queues)
        assigned = await self.redis.keys(RedisKeys.assigned("*"))
        if len(assigned) > 0:
            await self.redis.delete(*assigned)
        params = await self.redis.keys(RedisKeys.parameters("*", "*"))
        if len(params) > 0:
            await self.redis.delete(*params)
        await self.redis.aclose()


ctrl: Controller


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    global ctrl
    ctrl = Controller()
    run_task = asyncio.create_task(ctrl.run())
    run_task.add_done_callback(done_callback)
    yield
    run_task.cancel()
    await ctrl.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.get("/api/v1/config")
async def get_configs() -> EnsembleState:
    global ctrl
    return await ctrl.get_configs()


@app.get("/api/v1/status")
async def get_status() -> dict[str, Any]:
    global ctrl
    return {
        "work_completed": ctrl.completed,
        "last_assigned": ctrl.mapping.complete_events,
        "assignment": ctrl.mapping.assignments,
        "completed_events": ctrl.completed_events,
        "finished": len(ctrl.completed_events) == ctrl.mapping.len(),
        "processing_times": ctrl.worker_timing,
    }


@app.get("/api/v1/load")
async def get_load(
    intervals: Annotated[list[int] | None, Query()] = None, scan: bool = True
) -> SystemLoadType:
    global ctrl
    if intervals is None:
        intervals = [1, 10]
    return await ctrl.get_load(intervals, scan)


@app.get("/api/v1/progress")
async def get_progress() -> dict[str, Any]:
    global ctrl
    return {
        "last_assigned": ctrl.mapping.complete_events,
        "completed_events": len(ctrl.completed_events),
        "total_events": ctrl.mapping.len(),
        "finished": len(ctrl.completed_events) == ctrl.mapping.len(),
    }


@app.post("/api/v1/mapping")
async def set_mapping(
    mapping: Dict[StreamName, List[Optional[List[VirtualWorker]]]],
    all_wrap: bool = True,
) -> UUID4 | str:
    global ctrl
    config = await ctrl.get_configs()
    if set(mapping.keys()) - set(config.get_streams()) != set():
        return (
            f"streams {set(mapping.keys()) - set(config.get_streams())} not available"
        )
    m = Mapping(mapping, add_start_end=all_wrap)
    if len(config.workers) < m.min_workers():
        return f"only {len(config.workers)} workers available, but {m.min_workers()} required"
    await ctrl.set_mapping(m)
    return m.uuid


@app.get("/api/v1/mapping")
async def get_mapping() -> Dict[StreamName, List[Optional[List[VirtualWorker]]]]:
    global ctrl
    return ctrl.mapping.mapping


@app.post("/api/v1/sardana_hook")
async def set_sardana_hook(
    info: Dict[Literal["streams"] | Literal["scan"], Any]
) -> UUID4 | str:
    global ctrl
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
    m = Mapping.from_uniform(
        set(config.get_streams()).intersection(set(info["streams"])),
        info["scan"]["nb_points"],
    )
    logger.debug("set mapping")
    await ctrl.set_mapping(m)
    return m.uuid


@app.post("/api/v1/parameter/{name}")
async def set_param(request: Request, name: ParameterName) -> Digest:
    data = await request.body()
    logger.warning("got %s: %s", name, data)
    u = await ctrl.set_param(name, data)
    return u


@app.get("/api/v1/parameter/{name}")
async def get_param(name: ParameterName) -> Response:
    if name not in ctrl.parameters:
        raise HTTPException(status_code=404, detail="Parameter not found")

    data = ctrl.parameters[name].data
    return Response(data, media_type="application/x.bytes")


@app.get("/api/v1/parameters")
async def param_descr() -> list[ParameterType]:
    global ctrl
    return await ctrl.describe_parameters()
