from contextlib import nullcontext, contextmanager
import gzip
import json
import logging
import os
import pickle
import random
import threading
import time
import traceback
from typing import ContextManager, Iterator, Any, Optional, IO, Tuple

import cbor2
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import TypeAdapter
from pydantic_core import Url
from starlette.requests import Request
from starlette.responses import Response

from dranspose.helpers import utils
from dranspose.event import (
    InternalWorkerMessage,
    EventData,
    ResultData,
    message_tag_hook,
)
from dranspose.helpers.h5dict import router
from dranspose.protocol import (
    WorkerName,
    HashDigest,
    WorkParameter,
    ParameterName,
    ReducerState,
    WorkerState,
)


def get_internals(filename: os.PathLike[Any] | str) -> Iterator[InternalWorkerMessage]:
    f: IO[bytes]
    if str(filename).endswith(".gz"):
        f = gzip.open(filename, "rb")  # type: ignore[assignment]
        filename = str(filename)[:-3]
    else:
        f = open(filename, "rb")

    while True:
        try:
            if str(filename).endswith(".pkls"):
                frames = pickle.load(f)
            else:
                frames = cbor2.load(f, tag_hook=message_tag_hook)
            assert isinstance(frames, InternalWorkerMessage)
            yield frames
        except EOFError:
            break
    f.close()


logger = logging.getLogger(__name__)

ParamList = TypeAdapter(list[WorkParameter])

reducer_app = FastAPI()

reducer: Any | None = None


def get_data() -> Tuple[dict[str, Any], ContextManager[None]]:
    global reducer
    data = {}
    lock = nullcontext()
    if reducer is not None:
        if hasattr(reducer, "publish"):
            data = reducer.publish
        if hasattr(reducer, "publish_rlock"):
            lock = reducer.publish_rlock
    return data, lock


reducer_app.state.get_data = get_data

reducer_app.include_router(router)


@reducer_app.post("/api/v1/parameter/{name}")
async def post_param(request: Request, name: ParameterName) -> HashDigest:
    data = await request.body()
    logger.info("got %s: %s (len %d)", name, data[:100], len(data))
    param = WorkParameter(name=name, data=data)
    if name in reducer_app.state.param_description:
        param_desc = reducer_app.state.param_description[name]
        param.value = param_desc.from_bytes(data)
    reducer_app.state.parameters[name] = param
    return HashDigest("")


@reducer_app.get("/api/v1/parameter/{name}")
async def get_param(name: ParameterName) -> Response:
    if name not in reducer_app.state.parameters:
        raise HTTPException(status_code=404, detail="Parameter not found")

    data = reducer_app.state.parameters[name].data
    return Response(data, media_type="application/x.bytes")


class Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        pass

    @contextmanager
    def run_in_thread(self, port: Optional[int]) -> Iterator[None]:
        if port is None:
            yield
            return
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            while not self.started:
                time.sleep(1e-3)
            yield
        finally:
            self.should_exit = True
            thread.join()


def get_parameters(
    parameter_file: Optional[os.PathLike[Any] | str], workercls: type, reducercls: type
) -> dict[ParameterName, WorkParameter]:
    parameters = {}
    logger.info("loading parameters from file %s", parameter_file)
    if parameter_file is not None:
        try:
            with open(parameter_file) as f:
                parameters = {
                    ParameterName(p.name): p for p in ParamList.validate_json(f.read())
                }
        except UnicodeDecodeError:
            logger.info("parameter file is not a json, try pickle")
            try:
                with open(parameter_file, "rb") as fb:
                    plist = pickle.load(fb)
                    parameters = {
                        ParameterName(p.name): p
                        for p in ParamList.validate_python(plist)
                    }
            except Exception as e:
                logger.warning(
                    "parameter file is not a pickle: %s, try cbor", e.__repr__()
                )
                with open(parameter_file, "rb") as fb:
                    plist = cbor2.load(fb)
                    parameters = {
                        ParameterName(p.name): p
                        for p in ParamList.validate_python(plist)
                    }

    logger.info("params from file %s", parameters)

    param_description = {}
    if hasattr(workercls, "describe_parameters"):
        param_description.update({p.name: p for p in workercls.describe_parameters()})
    if hasattr(reducercls, "describe_parameters"):
        param_description.update({p.name: p for p in reducercls.describe_parameters()})
    logger.info("parameter descriptions %s", param_description)

    for p in parameters:
        if p in param_description:
            parameters[p].value = param_description[p].from_bytes(parameters[p].data)
    logger.info("parsed params are %s", parameters)

    for p in param_description:
        if p not in parameters:
            parameters[p] = WorkParameter(
                name=p,
                value=param_description[p].default,
                data=param_description[p].to_bytes(param_description[p].default),
            )

    reducer_app.state.param_description = param_description
    logger.info("final params are %s", parameters)
    return parameters


def timer(red: Any) -> None:
    while True:
        delay = 1
        if hasattr(red, "timer"):
            delay = red.timer(reducer_app.state.parameters)

        time.sleep(delay)


def _work_event(
    worker: Any,
    index: int,
    reducer: Any,
    event: EventData,
    parameters: dict[ParameterName, WorkParameter],
    tick: bool,
) -> None:
    data = worker.process_event(event, parameters, tick)
    if data is None:
        return
    rd = ResultData(
        event_number=event.event_number,
        worker=WorkerName(f"replay-worker-{index}"),
        payload=data,
        parameters_hash=HashDigest(
            "688787d8ff144c502c7f5cffaafe2cc588d86079f9de88304c26b0cb99ce91c6"
        ),
    )

    header = rd.model_dump_json(exclude={"payload"}).encode("utf8")
    body = pickle.dumps(rd.payload)
    prelim = json.loads(header)
    prelim["payload"] = pickle.loads(body)
    result = ResultData.model_validate(prelim)

    reducer.process_result(result, parameters)


def _finish(
    workers: list[Any], reducer: Any, parameters: dict[ParameterName, WorkParameter]
) -> None:
    for worker in workers:
        if hasattr(worker, "finish"):
            try:
                worker.finish(parameters=parameters)
            except Exception as e:
                logger.error(
                    "worker finished failed with %s\n%s",
                    e.__repr__(),
                    traceback.format_exc(),
                )

    if hasattr(reducer, "finish"):
        try:
            reducer.finish(parameters=parameters)
        except Exception as e:
            logger.error(
                "reducer finish failed with %s\n%s",
                e.__repr__(),
                traceback.format_exc(),
            )


def replay(
    wclass: str,
    rclass: str,
    zmq_files: Optional[list[os.PathLike[Any] | str]] = None,
    source: Optional[str] = None,
    parameter_file: Optional[os.PathLike[Any] | str] = None,
    port: Optional[int] = None,
    stop_event: threading.Event | None = None,
    nworkers: int = 2,
    broadcast_first: bool = True,
    done_event: threading.Event | None = None,
    start_event: threading.Event | None = None,
    latency: float | None = None,
) -> None:
    if source is not None:
        sourcecls = utils.import_class(source)
        inst = sourcecls()
        gens = inst.get_source_generators()
    elif zmq_files is not None:
        gens = [get_internals(f) for f in zmq_files]
    else:
        gens = []

    workercls = utils.import_class(wclass)
    logger.info("custom worker class %s", workercls)

    reducercls = utils.import_class(rclass)
    logger.info("custom reducer class %s", reducercls)

    reducer_app.state.parameters = get_parameters(parameter_file, workercls, reducercls)

    logger.info("use parameters %s", reducer_app.state.parameters)

    global reducer
    workers = []
    for wi in range(nworkers):
        wstate = WorkerState(name=WorkerName(f"replay-worker-{wi}"))
        wobj = workercls(
            parameters=reducer_app.state.parameters, context={}, state=wstate
        )
        workers.append(wobj)

    rstate = ReducerState(url=Url("tcp://localhost:10200"))
    reducer = reducercls(
        parameters=reducer_app.state.parameters, context={}, state=rstate
    )

    logger.info("created workers %s", workers)
    threading.Thread(target=timer, daemon=True, args=(reducer,)).start()

    config = uvicorn.Config(
        reducer_app, port=port or 5000, host="localhost", log_level="info"
    )
    server = Server(config)
    # server.run()

    first = True

    with server.run_in_thread(port):
        cache = [None for _ in gens]
        last_tick = 0.0
        if start_event is not None:
            start_event.wait()
        while True:
            try:
                internals = [
                    next(gen) if ch is None else ch for gen, ch in zip(gens, cache)
                ]
                if len(internals) == 0:
                    break
                lowestevn = min([ev.event_number for ev in internals])
                lowinternals = []
                cache = internals
                for idx, ie in enumerate(internals):
                    if ie.event_number == lowestevn:
                        lowinternals.append(ie)
                        cache[idx] = None
                event = EventData.from_internals(lowinternals)

                dst_worker_ids = [random.randint(0, len(workers) - 1)]
                if first and broadcast_first:
                    dst_worker_ids = list(range(len(workers)))
                    first = False

                for wi in dst_worker_ids:
                    logger.info("ev %d spread to wi %d", event.event_number, wi)
                    tick = False
                    if hasattr(workers[wi], "get_tick_interval"):
                        interval_s = workers[wi].get_tick_interval(
                            parameters=reducer_app.state.parameters
                        )
                        if last_tick + interval_s < time.time():
                            tick = True
                            last_tick = time.time()
                    _work_event(
                        workers[wi],
                        wi,
                        reducer,
                        event,
                        reducer_app.state.parameters,
                        tick,
                    )
                if latency is not None:
                    time.sleep(latency)
            except StopIteration:
                logger.debug("end of replay, calling finish")
                _finish(workers, reducer, reducer_app.state.parameters)
                break
        if done_event is not None:
            done_event.set()
        logger.info("check if webserver should stay alive %s", stop_event)
        if stop_event is None:
            stop_event = threading.Event()
            stop_event.set()
        else:
            print("press ctrl-C to stop")
        try:
            logger.info("waiting for event")
            stop_event.wait()
        except KeyboardInterrupt:
            pass
        logger.info("replay finished")
