from datetime import datetime
from types import UnionType

import zmq

from dranspose.data.positioncap import (
    PositionCapStart,
    PositionCapEnd,
    PositionCapValues,
    PositionCapField,
)
from dranspose.event import StreamData


class PositioncapParser:
    def __init__(self):
        self.arm_time = None
        self.fields = []

    def parse(self, data: StreamData) -> UnionType:
        """
        Parses a position capture packet

        Arguments:
            data: a frame comming from pcap

        Returns:
            a position capture
        """
        assert data.typ == "PCAP"
        assert data.length == 1
        frame = data.frames[0]
        if isinstance(frame, zmq.Frame):
            val = frame.bytes.decode()
        else:
            val = frame.decode()

        if val.startswith("arm_time:"):
            # header
            for line in val.split("\n"):
                if line.startswith("arm_time: "):
                    self.arm_time = datetime.fromisoformat(line[len("arm_time: ") :])
                if line.startswith(" "):
                    parts = line.strip().split(" ")
                    self.fields.append({"name": parts[0], "type": parts[1]})
            return PositionCapStart(arm_time=self.arm_time)
        elif val.startswith("END"):
            return PositionCapEnd()
        else:
            parts = val.strip().split(" ")
            data = {}
            for f, v in zip(self.fields, parts):
                if f["type"] == "uint32":
                    data[f["name"]] = PositionCapField(
                        value=int(v), type=f["type"], name=f["name"]
                    )
                if f["type"] == "double":
                    data[f["name"]] = PositionCapField(
                        value=float(v), type=f["type"], name=f["name"]
                    )
            return PositionCapValues(fields=data)
