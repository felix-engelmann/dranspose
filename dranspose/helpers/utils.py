import importlib
import os
import sys


def import_class(path: str) -> type:
    sys.path.append(os.getcwd())
    module = importlib.import_module(path.split(":")[0])
    custom = getattr(module, path.split(":")[1])
    return custom
