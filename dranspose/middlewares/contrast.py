import pickle
from typing import Any

import zmq

from dranspose.event import StreamData


def parse(data: StreamData) -> dict[str, Any]:
    """
    Parses a contrast packet, which returns a dict, depending on the status of contrast


    Heartbeat messages are discarded.

    Arguments:
        data: a frame comming from contrast

    Returns:
        a python dictionary or OrderedDict containing the current status
    """
    assert data.typ == "contrast"
    assert data.length == 1
    frame = data.frames[0]
    if isinstance(frame, zmq.Frame):
        return pickle.loads(frame.bytes)
    else:
        return pickle.loads(frame)
