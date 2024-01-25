import json
from types import UnionType

import zmq

from dranspose.data.sardana import SardanaPacket
from dranspose.event import StreamData


def parse(data: StreamData) -> UnionType:
    """
    Parses a sardana packet, which returns a dict

    Arguments:
        data: a frame comming from sardana

    Returns:
        a SardanaPacket
    """
    assert data.typ == "sardana"
    assert data.length == 1
    frame = data.frames[0]
    if isinstance(frame, zmq.Frame):
        val = json.loads(frame.bytes)
    else:
        val = json.loads(frame)

    return SardanaPacket.validate_python(val)
