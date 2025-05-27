import logging
from contextlib import nullcontext
from types import SimpleNamespace
from typing import ContextManager, Any, cast
from fastapi import Request
from starlette.responses import Response

from dranspose.helpers.h5dict import datasets, _path_to_uuid, values, attribute
from dranspose.helpers.h5types import H5SimpleShape


def fake_state() -> Request:
    req = SimpleNamespace()
    setattr(req, "app", SimpleNamespace())
    setattr(req, "headers", dict())
    setattr(req.app, "state", SimpleNamespace())
    setattr(req.app.state, "get_data", None)
    return cast(Request, req)


def test_datasets() -> None:
    req = fake_state()

    target = [[1, 2], [3, 4], [5, 6]]

    def get_data() -> tuple[dict[str, Any], ContextManager[None]]:
        return {"bla": target}, nullcontext()

    req.app.state.get_data = get_data

    uuid = _path_to_uuid(["bla"], "g-")
    r = datasets(req, uuid)
    logging.info("ret %s", r.model_dump_json())
    assert isinstance(r.shape, H5SimpleShape)
    assert r.shape.dims == [3, 2]
    target.append([-1, -2])
    assert r.shape.dims == [3, 2]
    logging.info("ret %s", r.model_dump_json())


def test_value_strlist() -> None:
    req = fake_state()

    target = ["hi", "there"]

    def get_data() -> tuple[dict[str, Any], ContextManager[None]]:
        return {"bla": target}, nullcontext()

    req.app.state.get_data = get_data

    uuid = _path_to_uuid(["bla"], "d-")
    r = values(req, uuid)
    logging.info("ret %s", r)
    assert r == {"value": ["hi", "there"]}
    target[1] = "hippo"
    assert r == {"value": ["hi", "there"]}


def test_value_intlist() -> None:
    req = fake_state()

    target = [1, 2, 3, 4]

    def get_data() -> tuple[dict[str, Any], ContextManager[None]]:
        return {"bla": target}, nullcontext()

    req.app.state.get_data = get_data

    uuid = _path_to_uuid(["bla"], "d-")
    r = values(req, uuid)
    assert isinstance(r, Response)
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


def test_attribute() -> None:
    req = fake_state()

    target = [1, 2, 3, 4]

    def get_data() -> tuple[dict[str, Any], ContextManager[None]]:
        return {"bla": 2, "bla_attrs": {"test": target}}, nullcontext()

    req.app.state.get_data = get_data

    uuid = _path_to_uuid(["bla"], "d-")
    r = attribute(req, "datasets", uuid, "test")
    logging.info("ret %s", r)
    assert r.value == [1, 2, 3, 4]
    assert isinstance(r.shape, H5SimpleShape)
    assert r.shape.dims == [4]
    target[1] = 5
    target.append(10)
    assert r.value == [1, 2, 3, 4]
    assert r.shape.dims == [4]
