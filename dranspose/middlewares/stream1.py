import json
from types import UnionType

import numpy as np
import zmq

from dranspose.data.stream1 import Stream1Packet, Stream1Data
from dranspose.event import StreamData


def parse(data: StreamData) -> UnionType:
    """
    Parses a stream1 packet, which returns a start,image or end message

    Arguments:
        data: a frame comming from a stream1 source

    Returns:
        a Stream1Packet
    """
    assert data.typ == "STINS"
    frame = data.frames[0]
    if isinstance(frame, zmq.Frame):
        val = json.loads(frame.bytes)
    elif isinstance(frame, bytes):
        val = json.loads(frame)
    else:
        raise Exception("invalid StreamData")

    packet = Stream1Packet.validate_python(val)
    print("packet", packet)
    if isinstance(packet, Stream1Data):
        assert data.length == 2
        bufframe = data.frames[1]
        if isinstance(bufframe, zmq.Frame):
            bufframe = bufframe.bytes
        buf = np.frombuffer(bufframe, dtype=packet.type)
        img = buf.reshape(packet.shape)
        packet.data = img

    return packet
