import json
from types import UnionType

import zmq

from dranspose.data.pcap import PCAPPacket
from dranspose.event import StreamData


def parse(data: StreamData) -> UnionType:
    """
    Parses a pcap packet

    Arguments:
        data: a frame comming from the pcap tango device

    Returns:
        a PCAPPacket
    """
    assert data.typ == "PCAP"
    assert data.length == 1
    frame = data.frames[0]
    if isinstance(frame, zmq.Frame):
        val = json.loads(frame.bytes)
    else:
        val = json.loads(frame)

    return PCAPPacket.validate_python(val)
