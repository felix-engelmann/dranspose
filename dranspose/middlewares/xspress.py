import json
import pickle
from types import UnionType
from typing import Any

import numpy as np
import zmq
from numpy import ndarray

from dranspose.data.xspress3 import (
    XspressPacket,
    XspressImage,
)
from dranspose.event import StreamData


def parse(
    data: StreamData,
) -> UnionType:
    """
    Parses a Xspress3 packet, which either gives a start/end message or a tuple with a spectra array

    Arguments:
        data: a frame comming from the Xspress3 tango device

    Returns:
        an XspressPacket
    """
    assert data.typ == "xspress"
    assert data.length >= 1
    headerframe = data.frames[0]
    if isinstance(headerframe, zmq.Frame):
        headerframe = headerframe.bytes
    packet = XspressPacket.validate_json(headerframe)
    print("packets", packet)
    if isinstance(packet, XspressImage):
        assert data.length == 3
        bufframe = data.frames[1]
        if isinstance(bufframe, zmq.Frame):
            bufframe = bufframe.bytes
        buf = np.frombuffer(bufframe, dtype=packet.type)
        img = buf.reshape(packet.shape)
        metaframe = data.frames[2]
        if isinstance(metaframe, zmq.Frame):
            metaframe = metaframe.bytes
        meta = pickle.loads(metaframe)
        # meta description: ocr[0], AllEvents[0], AllGood[0], ClockTicks[0],
        #                   TotalTicks[0], ResetTicks[0], event_widths, dtc[0]]
        meta = {
            k: v
            for k, v in zip(
                [
                    "ocr",
                    "AllEvents",
                    "AllGood",
                    "ClockTicks",
                    "TotalTicks",
                    "ResetTicks",
                    "event_widths",
                    "dtc",
                ],
                meta,
            )
        }
        packet.data = img
        packet.meta = meta

    return packet
