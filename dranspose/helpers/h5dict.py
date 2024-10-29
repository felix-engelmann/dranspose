import base64
import logging
import time
from typing import Any, Literal

import numpy as np
from fastapi import APIRouter, FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import Response

from dranspose.helpers.h5types import (
    H5Root,
    H5Group,
    H5Attribute,
    H5Shape,
    H5Type,
    H5NumType,
    H5StrType,
    H5ValuedAttribute,
    H5ScalarShape,
    H5SimpleShape,
    H5CompType,
    H5NamedType,
    H5Link,
    H5Dataset,
)

router = APIRouter()

# only activate for running it directly, otherwise it overwrites cli.py basic config
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def _get_now() -> float:
    return time.time()


def _path_to_uuid(path: list[str]) -> bytes:
    abspath = "/" + "/".join(path)
    # print("abspath", abspath)
    uuid = base64.b16encode(abspath.encode())
    # print("uuid from function is", str(id(obj)).encode() + b"-" + uuid)
    return b"h5dict-" + uuid


def _get_obj_at_path(
    data: dict[str, Any], path: str | list[str]
) -> tuple[Any, list[str]]:
    obj = data
    clean_path = []
    if isinstance(path, str):
        path = path.split("/")
    for p in path:
        # print("path part is:", p)
        if p == "":
            continue
        # print("traverse", obj)
        obj = obj[p]
        clean_path.append(p)
    return obj, clean_path


def _uuid_to_obj(data: dict[str, Any], uuid: str) -> tuple[Any, list[str]]:
    logger.debug("parse %s", uuid)
    idstr, path = uuid.split("-")
    path = base64.b16decode(path).decode()
    logger.debug("raw path %s", path)
    # assert id(data) == int(idstr)
    return _get_obj_at_path(data, path)


def _canonical_to_h5(canonical: str) -> H5NumType | None:
    if canonical.startswith(">"):
        order = "BE"
    else:
        order = "LE"
    bytelen = int(canonical[2:])
    htyp = None
    if canonical[1] == "f":
        # floating
        htyp = H5NumType(
            **{"class": "H5T_FLOAT", "base": f"H5T_IEEE_F{8 * bytelen}{order}"}
        )
    elif canonical[1] in ["u", "i"]:
        signed = canonical[1].upper()
        htyp = H5NumType(
            **{"class": "H5T_INTEGER", "base": f"H5T_STD_{signed}{8 * bytelen}{order}"}
        )
    else:
        logger.error("numpy type %s not available", canonical)

    return htyp


def _make_shape_type(obj: Any) -> tuple[H5Shape, H5Type]:
    h5shape = None
    h5type = None
    if type(obj) is int:
        h5shape = H5ScalarShape()
        h5type = H5NumType(**{"class": "H5T_INTEGER", "base": "H5T_STD_I64LE"})
    elif type(obj) is float:
        h5shape = H5ScalarShape()
        h5type = H5NumType(**{"class": "H5T_FLOAT", "base": "H5T_IEEE_F64LE"})

    elif type(obj) is str:
        h5shape = H5ScalarShape()
        h5type = H5StrType()

    elif isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], str):
        h5shape = H5SimpleShape(dims=[len(obj)])
        h5type = H5StrType()
    else:
        try:
            arr = np.array(obj)
            h5shape = H5SimpleShape(dims=arr.shape)

            if len(arr.dtype.descr) > 1:
                # compound datatype
                fields = []
                for field in arr.dtype.descr:
                    fields.append(
                        H5NamedType(name=field[0], type=_canonical_to_h5(field[1]))
                    )
                h5type = H5CompType(fields=fields)
            else:
                canonical = arr.dtype.descr[0][1]
                h5type = _canonical_to_h5(canonical)
            logging.debug("convert np dtype %s  to %s", arr.dtype, h5type)

        except Exception as e:
            logger.error("esception in handling code %s", e.__repr__())

    return h5shape, h5type


def _dataset_from_obj(data, path, obj, uuid):
    shape, typ = _make_shape_type(obj)
    # print("shape for ds", shape, "typ for ds", typ)
    ret = H5Dataset(
        id=uuid, attributeCount=len(_get_obj_attrs(data, path)), shape=shape, type=typ
    )
    return ret


@router.get("/datasets/{uuid}/value")
def values(req: Request, uuid, select: str | None = None):
    data = req.app.state.get_data()
    obj, _ = _uuid_to_obj(data, uuid)
    logger.debug("return value for obj %s", obj)
    logger.debug("selection %s", select)

    slices = []
    if select is not None:
        dims = select[1:-1].split(",")
        slices = [slice(*map(int, dim.split(":"))) for dim in dims]

    logger.debug("slices %s", slices)
    ret = obj
    if len(slices) == 1:
        ret = ret[slices[0]]
    elif len(slices) > 1:
        ret = ret[*slices]

    if type(ret) is np.ndarray:
        by = ret.tobytes()
        return Response(content=by, media_type="application/octet-stream")

    logger.debug("return value %s of type %s", ret, type(ret))
    return {"value": ret}


@router.get("/datasets/{uuid}")
def datasets(req: Request, uuid):
    data = req.app.state.get_data()
    obj, path = _uuid_to_obj(data, uuid)
    logger.debug("return dataset for obj %s", obj)
    ret = _dataset_from_obj(data, path, obj, uuid)
    # print("return dset", ret)
    return ret


def _get_group_link(obj, path):
    if isinstance(obj, dict):
        collection = "groups"
    else:
        collection = "datasets"
    return H5Link(collection=collection, id=_path_to_uuid(path), title=path[-1])


def _get_group_links(obj, path):
    if isinstance(obj, dict):
        links = []
        for key, val in obj.items():
            if not isinstance(key, str):
                logger.warning("unable to use non-string key: %s as group name", key)
                continue
            if key.endswith("_attrs"):
                continue
            link = _get_group_link(val, path + [key])
            links.append(link)
        return links


@router.get("/groups/{uuid}/links/{name}")
def link(req: Request, uuid, name):
    data = req.app.state.get_data()
    obj, path = _uuid_to_obj(data, uuid)
    path.append(name)
    if name in obj:
        obj = obj[name]
        ret = {"link": _get_group_link(obj, path)}
        logger.debug("link name is %s", ret)
        return ret
    raise HTTPException(status_code=404, detail="Link not found")


@router.get("/groups/{uuid}/links")
def links(req: Request, uuid: str):
    data = req.app.state.get_data()
    obj, path = _uuid_to_obj(data, uuid)
    ret = {"links": _get_group_links(obj, path)}

    logger.debug("group links %s", ret)
    return ret


def _get_attr(aobj, name, include_values=True) -> H5Attribute | H5ValuedAttribute:
    # print("get attribute")
    if name in aobj:
        h5shape, h5type = _make_shape_type(aobj[name])
        # print("shape and type ret", h5shape, h5type)
        ret = H5Attribute(name=name, shape=h5shape, type=h5type)
        if include_values:
            val = aobj[name]
            if isinstance(val, np.ndarray):
                val = val.tolist()
            ret = H5ValuedAttribute(**ret.model_dump(by_alias=True), value=val)
        return ret


def _make_attrs(aobj, include_values=False):
    # print("make attributes of ", aobj)
    attrs = []
    if isinstance(aobj, dict):
        for key, value in aobj.items():
            if not isinstance(key, str):
                logger.warning(
                    "unable to use non-string key: %s as attribute name", key
                )
                continue
            attrs.append(_get_attr(aobj, key, include_values=include_values))
    return attrs


def _get_obj_attrs(data, path, include_values=False):
    if len(path) == 0:
        # get root attributes
        if "_attrs" in data:
            return _make_attrs(data["_attrs"], include_values=include_values)
    else:
        # print("normal attr fetch")
        parent, _ = _get_obj_at_path(data, path[:-1])
        # print("parent is", parent)
        if f"{path[-1]}_attrs" in parent:
            # print( "make attrs for")
            return _make_attrs(
                parent[f"{path[-1]}_attrs"], include_values=include_values
            )
    return []


@router.get("/{typ}/{uuid}/attributes/{name}")
def attribute(req: Request, typ: Literal["groups", "datasets"], uuid: str, name: str):
    # print("get attr with name", typ, uuid, name)
    data = req.app.state.get_data()
    obj, path = _uuid_to_obj(data, uuid)
    logger.debug(
        "start listing attributes typ %s id %s, obj %s, path: %s", typ, uuid, obj, path
    )
    allattrs = _get_obj_attrs(data, path, include_values=True)
    for attr in allattrs:
        if attr.name == name:
            logger.debug("return attribute %s", attr)
            return attr
    raise HTTPException(status_code=404, detail="Attribute not found")


@router.get("/{typ}/{uuid}/attributes")
def attributes(req: Request, typ: Literal["groups", "datasets"], uuid: str):
    data = req.app.state.get_data()
    obj, path = _uuid_to_obj(data, uuid)
    logger.debug(
        "start listing attributes typ %s id %s, obj %s, path: %s", typ, uuid, obj, path
    )
    return {"attributes": _get_obj_attrs(data, path)}


@router.get("/groups/{uuid}")
def group(req: Request, uuid: str):
    data = req.app.state.get_data()
    obj, path = _uuid_to_obj(data, uuid)
    logger.debug("start listing group id %s, obj %s, path: %s", uuid, obj, path)

    if isinstance(obj, dict):
        linkCount = len(
            list(
                filter(
                    lambda x: isinstance(x, str) and not x.endswith("_attrs"),
                    obj.keys(),
                )
            )
        )
        group = H5Group(
            root=_path_to_uuid([]),
            id=uuid,
            linkCount=linkCount,
            attributeCount=len(_get_obj_attrs(data, path)),
        )
        logger.debug("group is %s", group)
        return group


@router.get("/")
def read_root(request: Request):
    logging.debug("data %s", request.app.state.get_data())
    data = request.app.state.get_data()
    if isinstance(data, dict):
        uuid = _path_to_uuid([])
        ret = H5Root(root=uuid)
        return ret


app = FastAPI()

app.include_router(router)


def get_data():
    dt = np.dtype({"names": ["a", "b"], "formats": [float, int]})

    arr = np.array([(0.5, 1)], dtype=dt)

    return {
        "live": 34,
        "other": {"third": [1, 2, 3]},  # only _attrs allowed in root
        "other_attrs": {"NX_class": "NXother"},
        "image": np.ones((1000, 1000)),
        "image_attrs": {"listattr": [42, 43, 44, 45]},
        "specialtyp": np.ones((10, 10), dtype=">u8"),
        "specialtyp_attrs": {"NXdata": "NXspecial"},
        "hello": "World",
        "_attrs": {"NX_class": "NXentry"},
        "composite": arr,
        "composite_attrs": {"axes": ["I", "q"]},
        "oned_attrs": {"NX_class": "NXdata", "axes": ["motor"], "signal": "data"},
        "oned": {
            "data": np.ones((42)),
            "data_attrs": {"long_name": "photons"},
            "motor": np.linspace(0, 1, 42),
            "motor_attrs": {"long_name": "motor name"},
        },
        "twod_attrs": {
            "NX_class": "NXdata",
            "axes": ["motor", "."],
            "signal": "frame",
            "interpretation": "image",
        },
        "twod": {
            "frame": np.ones((42, 42)),
            "motor": np.linspace(0, 1, 42),
            "motor_attrs": {"long_name": "motor name"},
        },
    }


app.state.get_data = get_data
