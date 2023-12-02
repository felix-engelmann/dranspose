import json
import logging
import os
import pickle
import uuid
from typing import Iterator, Any

from dranspose.helpers import utils
from dranspose.event import InternalWorkerMessage, EventData, ResultData
from dranspose.protocol import WorkerName


def get_internals(filename: os.PathLike[Any]) -> Iterator[InternalWorkerMessage]:
    with open(filename, "rb") as f:
        while True:
            try:
                frames = pickle.load(f)
                assert type(frames) == InternalWorkerMessage
                yield frames
            except EOFError:
                break


logger = logging.getLogger(__name__)


def replay(
    wclass: str,
    rclass: str,
    zmq_files: list[os.PathLike[Any]],
    parameter_file: os.PathLike[Any],
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
                parameters = json.load(f)
        except:
            with open(parameter_file, "rb") as fb:
                parameters = pickle.load(fb)

    worker = workercls(parameters)
    reducer = reducercls(parameters)

    while True:
        try:
            internals = [next(gen) for gen in gens]
            event = EventData.from_internals(internals)

            data = worker.process_event(event, parameters=parameters)

            rd = ResultData(
                event_number=event.event_number,
                worker=WorkerName("development"),
                payload=data,
                parameters_uuid=uuid.uuid4(),
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
            except:
                pass

            try:
                reducer.finish(parameters=parameters)
            except:
                pass
            break
