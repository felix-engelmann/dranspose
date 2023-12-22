import asyncio
import hashlib
import importlib
import logging
import os
import sys
import traceback
from asyncio import Future

from dranspose.protocol import Digest, WorkParameter, ParameterName


def import_class(path: str) -> type:
    sys.path.append(os.getcwd())
    module = importlib.import_module(path.split(":")[0])
    custom = getattr(module, path.split(":")[1])
    return custom


def parameters_hash(parameters: dict[ParameterName, WorkParameter]) -> Digest:
    m = hashlib.sha256()
    for n in sorted(parameters):
        m.update(parameters[n].uuid.bytes)
    return Digest(m.hexdigest())


def done_callback(futr: Future[None]) -> None:
    try:
        futr.result()
    except asyncio.exceptions.CancelledError:
        pass
    except Exception as e:
        logging.error(
            "subroutine crashed %s trace: %s",
            e.__repr__(),
            traceback.format_exc(),
        )
