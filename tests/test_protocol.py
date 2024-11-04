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
    assert [b"asd", b"lam1", b"lam2", b"lam3", b"alba"] == [
        b.bytes if isinstance(b, zmq.Frame) else b for b in buffers
    ]
    assert (
        dump[:148]
        == '{"event_number":42,"streams":{"orca":{"typ":"STINS","length":1},"lambda":{"typ":"STINS","length":3},"alba":{"typ":"STINS","length":1}},"created_at":'
    )
    prelim = json.loads(dump)
    assert prelim == {
        "event_number": 42,
        "streams": {
            "orca": {"typ": "STINS", "length": 1},
            "lambda": {"typ": "STINS", "length": 3},
            "alba": {"typ": "STINS", "length": 1},
        },
        "created_at": prelim["created_at"],
    }
    pos = 0
    for stream, data in prelim["streams"].items():
        print(stream, data)
        data["frames"] = buffers[pos : pos + data["length"]]
        assert data["typ"] == "STINS"
        pos += data["length"]
    assert pos == len(buffers)
    msg = InternalWorkerMessage.model_validate(prelim)
    assert msg.event_number == 42

    albaframe = msg.streams[StreamName("alba")].frames[0]
    if isinstance(albaframe, zmq.Frame):
        albaframe = albaframe.bytes
    lamframe = msg.streams[StreamName("lambda")].frames[2]
    if isinstance(lamframe, zmq.Frame):
        lamframe = lamframe.bytes
    orcaframe = msg.streams[StreamName("orca")].frames[0]
    if isinstance(orcaframe, zmq.Frame):
        orcaframe = orcaframe.bytes
    assert albaframe == b"alba"
    assert lamframe == b"lam3"
    assert orcaframe == b"asd"


def test_out_of_band_frames() -> None:
    buffers = [zmq.Frame(b"asd")]
    orca = StreamData(typ="STINS", frames=buffers)

    dump = orca.model_dump_json(exclude={"frames"})

    assert dump == '{"typ":"STINS","length":1}'

    prelim = json.loads(dump)
    assert prelim == {"typ": "STINS", "length": 1}
    prelim["frames"] = buffers
    sd = StreamData.model_validate(prelim)
    assert sd.frames == buffers
