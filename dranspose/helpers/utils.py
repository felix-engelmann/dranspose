import hashlib
import importlib
import os
import sys

from dranspose.protocol import Digest, WorkParameter, ParameterName


def import_class(path: str) -> type:
    sys.path.append(os.getcwd())
    module = importlib.import_module(path.split(":")[0])
    custom = getattr(module, path.split(":")[1])
    return custom


def parameters_hash(parameters: dict[ParameterName, WorkParameter]) -> Digest:
    m = hashlib.sha256()
    for n in parameters:
        m.update(parameters[n].uuid.bytes)
    return Digest(m.hexdigest())
