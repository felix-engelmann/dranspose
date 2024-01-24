import contextlib
import json
import logging
import os
import pickle
import threading
import time
from typing import Iterator, Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import TypeAdapter
from starlette.responses import Response

from dranspose.helpers import utils
from dranspose.event import InternalWorkerMessage, EventData, ResultData
from dranspose.helpers.jsonpath_slice_ext import NumpyExtentedJsonPathParser
from dranspose.protocol import WorkerName, Digest, WorkParameter


def get_internals(filename: os.PathLike[Any] | str) -> Iterator[InternalWorkerMessage]:
    with open(filename, "rb") as f:
        while True:
            try:
                frames = pickle.load(f)
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
        ret = [match.value for match in jsonpath_expr.find(reducer.publish)]  # type: ignore [union-attr]
        data = pickle.dumps(ret)
        return Response(data, media_type="application/x.pickle")
    except Exception as e:
        raise HTTPException(status_code=400, detail="malformed path %s" % e.__repr__())


class Server(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self, port):
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


def replay(
    wclass: str,
    rclass: str,
    zmq_files: list[os.PathLike[Any] | str],
    parameter_file: os.PathLike[Any] | str,
    port: Optional[int] = None,
    keepalive: bool = False,
) -> None:
    gens = [get_internals(f) for f in zmq_files]

    workercls = utils.import_class(wclass)
    logger.info("custom worker class %s", workercls)

    reducercls = utils.import_class(rclass)
    logger.info("custom reducer class %s", reducercls)

    parameters = {}
    if parameter_file:
        try:
            with open(parameter_file) as f:
                parameters = {p.name: p for p in ParamList.validate_json(f.read())}
        except UnicodeDecodeError:
            with open(parameter_file, "rb") as fb:
                parameters = pickle.load(fb)

    param_description = {}
    if hasattr(workercls, "describe_parameters"):
        param_description.update({p.name: p for p in workercls.describe_parameters()})
    if hasattr(reducercls, "describe_parameters"):
        param_description.update({p.name: p for p in reducercls.describe_parameters()})
    logger.info("parameter descriptions %s", param_description)

    for p in parameters:
        if p in param_description:
            parameters[p].value = param_description[p].from_bytes(parameters[p].data)

    logger.info("use parameters %s", parameters)

    global reducer
    worker = workercls(parameters=parameters)
    reducer = reducercls(parameters=parameters)

    config = uvicorn.Config(reducer_app, port=port, host="localhost", log_level="info")
    server = Server(config)
    # server.run()

    with server.run_in_thread(port):
        while True:
            try:
                internals = [next(gen) for gen in gens]
                event = EventData.from_internals(internals)

                data = worker.process_event(event, parameters=parameters)

                rd = ResultData(
                    event_number=event.event_number,
                    worker=WorkerName("development"),
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
                if hasattr(worker, "finish"):
                    try:
                        worker.finish(parameters=parameters)
                    except Exception as e:
                        logger.error("worker finished failed with %s", e.__repr__())

                if hasattr(reducer, "finish"):
                    try:
                        reducer.finish(parameters=parameters)
                    except Exception as e:
                        print("reducer finish failed with %s", e.__repr__())
                break
        if keepalive:
            input("press key to stop server")
