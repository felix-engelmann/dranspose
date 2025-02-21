import json
from types import UnionType

import zmq

from dranspose.data.lecroy import LecroyPacket
from dranspose.event import StreamData


def parse(data: StreamData) -> UnionType:
    """
    Parses a lecroy packet

    Arguments:
        data: a frame comming from the lecroy tango device

    Returns:
        a LecroyPacket
    """
    assert data.typ == "Lecroy"
    assert data.length == 1
    frame = data.frames[0]
    if isinstance(frame, zmq.Frame):
        val = json.loads(frame.bytes)
    else:
        val = json.loads(frame)

    return LecroyPacket.validate_python(val)
