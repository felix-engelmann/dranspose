import contextlib
import json
import logging
import os
import pickle
import random
import threading
import time
import traceback
from typing import Iterator, Any, Optional

import cbor2
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import TypeAdapter
from starlette.responses import Response

from dranspose.helpers import utils
from dranspose.event import (
    InternalWorkerMessage,
    EventData,
    ResultData,
    message_tag_hook,
)
from dranspose.helpers.jsonpath_slice_ext import NumpyExtentedJsonPathParser
from dranspose.parameters import ParameterBase
from dranspose.protocol import WorkerName, Digest, WorkParameter


def get_internals(filename: os.PathLike[Any] | str) -> Iterator[InternalWorkerMessage]:
    with open(filename, "rb") as f:
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


logger = logging.getLogger(__name__)

ParamList = TypeAdapter(list[WorkParameter])

reducer_app = FastAPI()

reducer = None


@reducer_app.get("/api/v1/result/{path:path}")
async def get_path(path: str) -> Any:
    global reducer
    if not hasattr(reducer, "publish"):
        raise HTTPException(status_code=404, detail="no publishable data")
    try:
        if path == "":
            path = "$"
        jsonpath_expr = NumpyExtentedJsonPathParser(debug=False).parse(path)
        print("expr", jsonpath_expr.__repr__())
        ret = [match.value for match in jsonpath_expr.find(reducer.publish)]  # type: ignore [attr-defined]
        data = pickle.dumps(ret)
        return Response(data, media_type="application/x.pickle")
    except Exception as e:
        raise HTTPException(status_code=400, detail="malformed path %s" % e.__repr__())


class Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        pass

    @contextlib.contextmanager
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
    parameter_file: os.PathLike[Any] | str, workercls: type, reducercls: type
) -> dict[str, WorkParameter]:
    parameters = {}
    if parameter_file:
        try:
            with open(parameter_file) as f:
                parameters = {p.name: p for p in ParamList.validate_json(f.read())}
        except UnicodeDecodeError:
            with open(parameter_file, "rb") as fb:
                parameters = pickle.load(fb)

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
                data=ParameterBase.to_bytes(param_description[p].default),
            )

    logger.info("final params are %s", parameters)
    return parameters


def replay(
    wclass: str,
    rclass: str,
    zmq_files: list[os.PathLike[Any] | str],
    parameter_file: os.PathLike[Any] | str,
    port: Optional[int] = None,
    keepalive: bool = False,
    nworkers: int = 1,
) -> None:
    gens = [get_internals(f) for f in zmq_files]

    workercls = utils.import_class(wclass)
    logger.info("custom worker class %s", workercls)

    reducercls = utils.import_class(rclass)
    logger.info("custom reducer class %s", reducercls)

    parameters = get_parameters(parameter_file, workercls, reducercls)

    logger.info("use parameters %s", parameters)

    global reducer
    workers = [workercls(parameters=parameters, context={}) for _ in range(nworkers)]
    reducer = reducercls(parameters=parameters, context={})

    config = uvicorn.Config(
        reducer_app, port=port or 5000, host="localhost", log_level="info"
    )
    server = Server(config)
    # server.run()

    with server.run_in_thread(port):
        while True:
            try:
                internals = [next(gen) for gen in gens]
                event = EventData.from_internals(internals)

                wi = random.randint(0, len(workers) - 1)
                data = workers[wi].process_event(event, parameters=parameters)

                rd = ResultData(
                    event_number=event.event_number,
                    worker=WorkerName(f"development{wi}"),
                    payload=data,
                    parameters_hash=Digest(
                        "688787d8ff144c502c7f5cffaafe2cc588d86079f9de88304c26b0cb99ce91c6"
                    ),
                )

                header = rd.model_dump_json(exclude={"payload"}).encode("utf8")
                body = pickle.dumps(rd.payload)
                prelim = json.loads(header)
                prelim["payload"] = pickle.loads(body)
                result = ResultData.model_validate(prelim)

                reducer.process_result(result, parameters=parameters)

            except StopIteration:
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
                break
        if keepalive:
            input("press key to stop server")
