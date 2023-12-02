import pickle
from types import UnionType
from typing import Any

import zmq

from dranspose.data.contrast import ContrastPacket
from dranspose.event import StreamData


def parse(data: StreamData) -> UnionType:
    """
    Parses a contrast packet, which returns a dict, depending on the status of contrast

    Arguments:
        data: a frame comming from contrast

    Returns:
        a ContrastPacket containing a.o. the current status
    """
    assert data.typ == "contrast"
    assert data.length == 1
    frame = data.frames[0]
    if isinstance(frame, zmq.Frame):
        val = pickle.loads(frame.bytes)
    else:
        val = pickle.loads(frame)

    return ContrastPacket.validate_python(val)
