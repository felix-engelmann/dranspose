import asyncio
import hashlib
import importlib
import logging
import os
import sys
import traceback
from asyncio import Future, Task
from typing import Any

from dranspose.protocol import HashDigest, WorkParameter, ParameterName


def import_class(path: str) -> type:
    sys.path.append(os.getcwd())
    module = importlib.import_module(path.split(":")[0])
    custom = getattr(module, path.split(":")[1])
    return custom


def parameters_hash(parameters: dict[ParameterName, WorkParameter]) -> HashDigest:
    m = hashlib.sha256()
    for n in sorted(parameters):
        m.update(parameters[n].uuid.bytes)
    return HashDigest(m.hexdigest())


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


async def cancel_and_wait(task: Task[Any] | Future[Any]) -> None:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except StopIteration:
        pass
    except Exception as e:
        logging.error("cancel and wait task raised %s", e.__repr__())
