import json
import pickle
from typing import Any

import numpy as np
import zmq
from numpy import ndarray

from dranspose.data.xspress3_stream import (
    XspressPacket,
    XspressStart,
    XspressImage,
    XspressEnd,
)
from dranspose.event import StreamData


def parse(
    data: StreamData,
) -> (
    dict[str, Any]
    | tuple[dict[str, Any], ndarray[Any, Any], dict[str, ndarray[Any, Any]]]
):
    assert data.typ == "xspress"
    assert data.length >= 1
    headerframe = data.frames[0]
    if isinstance(headerframe, zmq.Frame):
        headerframe = headerframe.bytes
    header = json.loads(headerframe)
    packet = XspressPacket.validate_python(header)
    if type(packet) is XspressStart:
        return header
    elif type(packet) is XspressImage:
        assert data.length == 3
        bufframe = data.frames[1]
        if isinstance(bufframe, zmq.Frame):
            bufframe = bufframe.bytes
        buf = np.frombuffer(bufframe, dtype=header["type"])
        img = buf.reshape(header["shape"])
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
        return header, img, meta
    elif type(packet) is XspressEnd:
        return header
    return {}
