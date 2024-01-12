import json
import logging
import os
import pickle
from typing import Iterator, Any

from pydantic import TypeAdapter

from dranspose.helpers import utils
from dranspose.event import InternalWorkerMessage, EventData, ResultData
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


def replay(
    wclass: str,
    rclass: str,
    zmq_files: list[os.PathLike[Any] | str],
    parameter_file: os.PathLike[Any] | str,
) -> None:
    gens = [get_internals(f) for f in zmq_files]

    workercls = utils.import_class(wclass)
    logger.info("custom worker class %s", workercls)

    reducercls = utils.import_class(rclass)
    logger.info("custom reducer class %s", reducercls)

    parameters = None
    if parameter_file:
        try:
            with open(parameter_file) as f:
                parameters = {p.name: p for p in ParamList.validate_json(f.read())}
        except UnicodeDecodeError:
            with open(parameter_file, "rb") as fb:
                parameters = pickle.load(fb)

    param_description = {}
    try:
        param_description.update({p.name: p for p in workercls.describe_parameters()})
    except AttributeError:
        pass
    try:
        param_description.update({p.name: p for p in reducercls.describe_parameters()})
    except AttributeError:
        pass
    logger.info("parameter descriptions %s", param_description)

    for p in parameters:
        if p in param_description:
            parameters[p].value = param_description[p].from_bytes(parameters[p].data)

    worker = workercls(parameters=parameters)
    reducer = reducercls(parameters=parameters)

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
            try:
                worker.finish(parameters=parameters)
            except Exception:
                pass

            try:
                reducer.finish(parameters=parameters)
            except Exception:
                pass
            break
