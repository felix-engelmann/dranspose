import json
from types import UnionType
from typing import Any

import zmq

from dranspose.data.eiger_legacy import (
    EigerLegacyHeader,
    EigerLegacyPacket,
    EigerLegacyImage,
)
from dranspose.event import StreamData


def _get_json(frame: zmq.Frame | bytes) -> Any:
    if isinstance(frame, zmq.Frame):
        val = json.loads(frame.bytes)
    elif isinstance(frame, bytes):
        val = json.loads(frame)
    else:
        raise Exception("invalid StreamData")
    return val


def parse(data: StreamData) -> UnionType:
    """
    Parses a eiger legacy packet, which returns a start,image or end message

    Arguments:
        data: a frame comming from a eiger legacy source

    Returns:
        a EigerLegacyPacket
    """
    assert data.typ == "EIGER_LEGACY"
    packet = EigerLegacyPacket.validate_python(_get_json(data.frames[0]))
    if isinstance(packet, EigerLegacyHeader):
        assert data.length == 9
        packet.info = _get_json(data.frames[1])
        packet.appendix = _get_json(data.frames[8])

    if isinstance(packet, EigerLegacyImage):
        assert data.length == 5
        info = _get_json(data.frames[1])
        buffer = data.frames[2]
        if isinstance(buffer, zmq.Frame):
            buffer = buffer.bytes
        info["buffer"] = buffer
        packet.data = info
        packet.config = _get_json(data.frames[3])

    return packet
