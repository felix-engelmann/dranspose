import pickle
from typing import Any

import zmq

from dranspose.event import StreamData


def parse(data: StreamData) -> dict[str, Any]:
    assert data.typ == "contrast"
    assert data.length == 1
    frame = data.frames[0]
    if isinstance(frame, zmq.Frame):
        return pickle.loads(frame.bytes)
    else:
        return pickle.loads(frame)
