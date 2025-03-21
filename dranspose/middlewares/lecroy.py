from typing import List
import numpy as np
import pickle
import logging

import zmq

from dranspose.data.lecroy import (
    LecroyData,
    LecroyPacket,
    LecroyParsed,
    LecroySeqStart,
    seqstart_to_parsed,
)
from dranspose.event import StreamData

logger = logging.getLogger(__name__)


def parse(data: StreamData) -> LecroyParsed:
    """
    Parses a lecroy packet

    Arguments:
        data: a frame comming from the lecroy tango device

    Returns:
        a LecroyPacket
    """

    assert data.typ == "lecroy"
    assert data.length >= 1, "wrong number of multiparts"
    start = data.frames[0]
    if isinstance(start, zmq.Frame):
        start = start.bytes
    start_pkt = LecroyPacket.validate_json(start)
    logger.debug("lecroy packet", start_pkt)

    assert isinstance(
        start_pkt, LecroySeqStart
    ), f"Cannot identify header frame: {start_pkt}"
    nch = len(start_pkt.channels)
    assert (
        data.length == 1 + nch * 3 + 1
    ), f"Unexpected number of frames ({data.length}) for {nch} channels"

    res = seqstart_to_parsed(start_pkt)
    for ich in range(nch):
        offset = 1 + ich * 3
        metaframe = data.frames[offset]
        if isinstance(metaframe, zmq.Frame):
            metaframe = metaframe.bytes
        meta_pkt = LecroyPacket.validate_json(metaframe)
        logger.debug("lecroy packet", meta_pkt)
        assert isinstance(
            meta_pkt, LecroyData
        ), f"Cannot identify trace metadata frame: {meta_pkt}"
        res.meta.append(meta_pkt)

        tracesframe = data.frames[offset + 1]
        if isinstance(tracesframe, zmq.Frame):
            tracesframe = tracesframe.bytes
        buf = np.frombuffer(tracesframe, dtype=meta_pkt.dtype)
        traces = buf.reshape(meta_pkt.shape)
        res.data.append(traces)

        tsframe = data.frames[offset + 2]
        if isinstance(tsframe, zmq.Frame):
            tsframe = tsframe.bytes
        ts: List[float] = pickle.loads(tsframe)
        res.timestamps.append(ts)

    return res
