import json
import logging
import pickle
import uuid
from typing import Iterator

from dranspose import utils
from dranspose.event import StreamData, InternalWorkerMessage, EventData, ResultData


def get_internals(filename) -> Iterator[InternalWorkerMessage]:
    with open(filename, "rb") as f:
        while True:
            try:
                frames = pickle.load(f)
                assert type(frames) == InternalWorkerMessage
                yield frames
            except EOFError:
                break

logger = logging.getLogger(__name__)

def replay(wclass, rclass, zmq_files, parameter_file):
    gens = [get_internals(f) for f in zmq_files]

    workercls = utils.import_class(wclass)
    logger.info("custom worker class %s", workercls)

    reducercls = utils.import_class(rclass)
    logger.info("custom reducer class %s", reducercls)

    worker = workercls()
    reducer = reducercls()

    parameters=None
    if parameter_file:
        try:
            with open(parameter_file) as f:
                parameters = json.load(f)
        except:
            with open(parameter_file, "rb") as f:
                parameters = pickle.load(f)

    while True:
        try:
            internals = [next(gen) for gen in gens]
            event = EventData.from_internals(internals)

            data = worker.process_event(event, parameters=parameters)

            rd = ResultData(
                event_number=event.event_number,
                worker=b'development',
                payload=data,
                parameters_uuid=uuid.uuid4(),
            )

            reducer.process_result(rd, parameters=parameters)

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