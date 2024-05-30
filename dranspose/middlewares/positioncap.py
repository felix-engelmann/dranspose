from datetime import datetime
from typing import Optional

import zmq

from dranspose.data.positioncap import (
    PositionCapStart,
    PositionCapEnd,
    PositionCapValues,
    PositionCapField,
    PositionCapPacketType,
)
from dranspose.event import StreamData


class PositioncapParser:
    def __init__(self) -> None:
        self.arm_time: Optional[datetime] = None
        self.fields: list[PositionCapField] = []

    def parse(self, data: StreamData) -> PositionCapPacketType:
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
                    self.fields.append(
                        PositionCapField(name=f"{parts[0]}.{parts[2]}", type=parts[1])
                    )
            if self.arm_time is None:
                raise Exception("unable to parse header")
            return PositionCapStart(arm_time=self.arm_time)
        elif val.startswith("END"):
            return PositionCapEnd()
        else:
            parts = val.strip().split(" ")
            ret = PositionCapValues()
            for f, v in zip(self.fields, parts):
                ret.fields[f.name] = f.parse(v)
            return ret
