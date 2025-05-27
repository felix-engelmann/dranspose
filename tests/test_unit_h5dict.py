import logging
from contextlib import nullcontext
from types import SimpleNamespace

from dranspose.helpers.h5dict import datasets, _path_to_uuid, values, attribute


def fake_state():
    req = SimpleNamespace()
    setattr(req, "app", SimpleNamespace())
    setattr(req, "headers", dict())
    setattr(req.app, "state", SimpleNamespace())
    setattr(req.app.state, "get_data", None)
    return req


def test_datasets():
    req = fake_state()

    target = [[1, 2], [3, 4], [5, 6]]

    def get_data():
        return {"bla": target}, nullcontext()

    req.app.state.get_data = get_data

    uuid = _path_to_uuid(["bla"], "g-")
    r = datasets(req, uuid)
    logging.info("ret %s", r.model_dump_json())
    assert r.shape.dims == [3, 2]
    target.append([-1, -2])
    assert r.shape.dims == [3, 2]
    logging.info("ret %s", r.model_dump_json())


def test_value_strlist():
    req = fake_state()

    target = ["hi", "there"]

    def get_data():
        return {"bla": target}, nullcontext()

    req.app.state.get_data = get_data

    uuid = _path_to_uuid(["bla"], "d-")
    r = values(req, uuid)
    logging.info("ret %s", r)
    assert r == {"value": ["hi", "there"]}
    target[1] = "hippo"
    assert r == {"value": ["hi", "there"]}


def test_value_intlist():
    req = fake_state()

    target = [1, 2, 3, 4]

    def get_data():
        return {"bla": target}, nullcontext()

    req.app.state.get_data = get_data

    uuid = _path_to_uuid(["bla"], "d-")
    r = values(req, uuid)
    logging.info("ret %s", r.body)
    assert (
        r.body
        == b"\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"
    )
    target[1] = 5
    assert (
        r.body
        == b"\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"
    )


def test_attribute():
    req = fake_state()

    target = [1, 2, 3, 4]

    def get_data():
        return {"bla": 2, "bla_attrs": {"test": target}}, nullcontext()

    req.app.state.get_data = get_data

    uuid = _path_to_uuid(["bla"], "d-")
    r = attribute(req, "datasets", uuid, "test")
    logging.info("ret %s", r)
    assert r.value == [1, 2, 3, 4]
    assert r.shape.dims == [4]
    target[1] = 5
    target.append(10)
    assert r.value == [1, 2, 3, 4]
    assert r.shape.dims == [4]
