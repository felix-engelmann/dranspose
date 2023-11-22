import json

import zmq

from dranspose.event import InternalWorkerMessage, StreamData
from dranspose.protocol import EventNumber, StreamName


def test_header_serialisation() -> None:
    orca = StreamData(typ="STINS", frames=[zmq.Frame(b"asd")])
    lam = StreamData(
        typ="STINS", frames=[zmq.Frame(b"lam1"), zmq.Frame(b"lam2"), zmq.Frame(b"lam3")]
    )
    alba = StreamData(typ="STINS", frames=[zmq.Frame(b"alba")])
    message = InternalWorkerMessage(event_number=EventNumber(42))
    message.streams[StreamName("orca")] = orca
    message.streams[StreamName("lambda")] = lam
    message.streams[StreamName("alba")] = alba
    dump = message.model_dump_json(
        exclude={"streams": {"__all__": "frames"}}
    )  #: {"frames"}})

    buffers = message.get_all_frames()
    print([b.bytes for b in buffers])
    print(dump)
    prelim = json.loads(dump)
    print("prelim", prelim)
    pos = 0
    for stream, data in prelim["streams"].items():
        print(stream, data)
        data["frames"] = buffers[pos : pos + data["length"]]
        pos += data["length"]
    msg = InternalWorkerMessage.model_validate(prelim)
    print(msg)
    assert msg.streams[StreamName("alba")].frames[0].bytes == b"alba"
    assert msg.streams[StreamName("lambda")].frames[2].bytes == b"lam3"
    assert msg.streams[StreamName("orca")].frames[0].bytes == b"asd"


def test_out_of_band_frames() -> None:
    buffers = [zmq.Frame(b"asd")]
    orca = StreamData(typ="STINS", frames=buffers)

    dump = orca.model_dump_json(exclude={"frames"})

    print(dump)

    prelim = json.loads(dump)
    print("prelim", prelim)
    prelim["frames"] = buffers
    sd = StreamData.model_validate(prelim)
    print(sd)
