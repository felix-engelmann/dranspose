import json
import pickle

import numpy as np

from dranspose.data.xspress3_stream import XspressPacket, XspressStart, XspressImage, XspressEnd
from dranspose.event import StreamData


def parse(data: StreamData):
    assert data.typ == "xspress"
    assert data.length >= 1
    packet = XspressPacket.validate_json(data.frames[0].bytes)
    header = json.loads(data.frames[0].bytes)
    if type(packet) is XspressStart:
        return header
    if type(packet) is XspressImage:
        assert data.length == 3
        buf = np.frombuffer(data.frames[1].bytes, dtype=header["type"])
        img = buf.reshape(header["shape"])
        meta = pickle.loads(data.frames[2])
        # meta description: ocr[0], AllEvents[0], AllGood[0], ClockTicks[0],
        #                   TotalTicks[0], ResetTicks[0], event_widths, dtc[0]]
        meta = {k:v for k,v in zip(["ocr", "AllEvents", "AllGood", "ClockTicks",
                           "TotalTicks", "ResetTicks", "event_widths", "dtc"],meta)}
        return header, img, meta
    elif type(packet) is XspressEnd:
        return header