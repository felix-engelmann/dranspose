import base64
from contextlib import nullcontext
import copy
import gzip
import logging
import time
from typing import Any, ContextManager, Literal, Tuple

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
    H5IntType,
    H5FloatType,
    H5StrType,
    H5ValuedAttribute,
    H5ScalarShape,
    H5SimpleShape,
    H5CompType,
    H5NamedType,
    H5Link,
    H5Dataset,
    H5UUID,
)

router = APIRouter()

# only activate for running it directly, otherwise it overwrites cli.py basic config
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def _path_to_uuid(path: list[str], collection_type: str) -> H5UUID:
    abspath = "/" + "/".join(path)
    # print("abspath", abspath)
    uuid = base64.b16encode(abspath.encode()).decode()
    # print("uuid from function is", str(id(obj)).encode() + b"-" + uuid)
    return H5UUID(collection_type + "h5dict-" + uuid)


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
    col_type, idstr, path = uuid.split("-")
    path = base64.b16decode(path).decode()
    logger.debug("raw path %s", path)
    # assert id(data) == int(idstr)
    return _get_obj_at_path(data, path)


def _canonical_to_h5(canonical: str) -> H5Type | None:
    if canonical.startswith(">"):
        order = "BE"
    else:
        order = "LE"
    bytelen = int(canonical[2:])
    htyp: H5Type | None = None
    if canonical[1] == "f":
        # floating
        htyp = H5FloatType(base=f"H5T_IEEE_F{8 * bytelen}{order}")
    elif canonical[1] in ["u", "i"]:
        signed = canonical[1].upper()
        htyp = H5IntType(base=f"H5T_STD_{signed}{8 * bytelen}{order}")
    else:
        logger.error("numpy type %s not available", canonical)

    return htyp


def _make_shape_type(obj: Any) -> tuple[H5Shape | None, H5Type | None]:
    h5shape: H5Shape | None = None
    h5type: H5Type | None = None
    if type(obj) is int:
        h5shape = H5ScalarShape()
        h5type = H5IntType(base="H5T_STD_I64LE")
    elif type(obj) is float:
        h5shape = H5ScalarShape()
        h5type = H5FloatType(base="H5T_IEEE_F64LE")

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
                    typ = _canonical_to_h5(field[1])
                    if typ is not None:
                        fields.append(H5NamedType(name=field[0], type=typ))
                h5type = H5CompType(fields=fields)
            else:
                canonical = arr.dtype.descr[0][1]
                h5type = _canonical_to_h5(canonical)
            logging.debug("convert np dtype %s  to %s", arr.dtype, h5type)

        except Exception as e:
            logger.error("esception in handling code %s", e.__repr__())

    return h5shape, h5type


def _dataset_from_obj(
    data: Any, path: list[str], obj: Any, uuid: H5UUID
) -> H5Dataset | None:
    shape, typ = _make_shape_type(obj)
    if shape is not None and typ is not None:
        ret = H5Dataset(
            id=uuid,
            attributeCount=len(_get_obj_attrs(data, path)),
            shape=shape,
            type=typ,
        )
        return ret
    return None


def _get_group_link(obj: Any, path: list[str]) -> H5Link:
    collection: Literal["datasets", "groups"]
    if isinstance(obj, dict):
        collection = "groups"
        collection_type = "g-"
    else:
        collection = "datasets"
        collection_type = "d-"
    return H5Link(
        collection=collection,
        id=_path_to_uuid(path, collection_type=collection_type),
        title=path[-1],
    )


def _get_group_links(obj: Any, path: list[str]) -> list[H5Link]:
    links = []
    if isinstance(obj, dict):
        for key, val in obj.items():
            if not isinstance(key, str):
                logger.warning("unable to use non-string key: %s as group name", key)
                continue
            if key.endswith("_attrs"):
                continue
            link = _get_group_link(val, path + [key])
            links.append(link)
    return links


def _get_attr(
    aobj: dict[str, Any], name: str, include_values: bool = True
) -> H5Attribute | H5ValuedAttribute | None:
    # print("get attribute")
    if name in aobj:
        h5shape, h5type = _make_shape_type(aobj[name])
        # print("shape and type ret", h5shape, h5type)
        if h5shape is not None and h5type is not None:
            ret = H5Attribute(name=name, shape=h5shape, type=h5type)
            if include_values:
                val = aobj[name]
                if isinstance(val, np.ndarray):
                    val = val.tolist()
                ret = H5ValuedAttribute(**ret.model_dump(by_alias=True), value=val)
            return ret
    return None


def _make_attrs(
    aobj: dict[str, Any], include_values: bool = False
) -> list[H5Attribute] | list[H5ValuedAttribute]:
    # print("make attributes of ", aobj)
    attrs = []
    if isinstance(aobj, dict):
        for key, value in aobj.items():
            if not isinstance(key, str):
                logger.warning(
                    "unable to use non-string key: %s as attribute name", key
                )
                continue
            attr = _get_attr(aobj, key, include_values=include_values)
            if attr is not None:
                attrs.append(attr)
    return attrs


def _get_obj_attrs(
    data: dict[str, Any], path: list[str], include_values: bool = False
) -> list[H5Attribute] | list[H5ValuedAttribute]:
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


@router.get("/datasets/{uuid}/value", response_model=None)
def values(
    req: Request, uuid: H5UUID, select: str | None = None
) -> dict[str, Any] | Response:
    data, lock = req.app.state.get_data()
    with lock:
        obj, _ = _uuid_to_obj(data, uuid)
        logger.debug("return value for obj %s", obj)
        logger.debug("selection %s", select)

        if type(obj) in [int, float, str]:
            return {"value": obj}

        elif isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], str):
            return {"value": copy.copy(obj)}
        else:
            ret = np.array(obj)
            slices = []
            if select is not None:
                dims = select[1:-1].split(",")
                slices = [slice(*map(int, dim.split(":"))) for dim in dims]

            logger.debug("slices %s", slices)

            if len(slices) == 1:
                ret = ret[slices[0]]
            elif len(slices) > 1:
                ret = ret[*slices]

            ret_bytes = ret.tobytes()
            if "gzip" in req.headers.get("Accept-Encoding", ""):
                if len(ret_bytes) > 1000:
                    compressed_data = gzip.compress(ret_bytes, compresslevel=7)

                    return Response(
                        content=compressed_data,
                        media_type="application/octet-stream",
                        headers={"Content-Encoding": "gzip"},
                    )
            return Response(content=ret_bytes, media_type="application/octet-stream")


@router.get("/datasets/{uuid}")
def datasets(req: Request, uuid: H5UUID) -> H5Dataset:
    data, lock = req.app.state.get_data()
    with lock:
        obj, path = _uuid_to_obj(data, uuid)
        logger.debug("return dataset for obj %s", obj)
        ret = _dataset_from_obj(data, path, obj, uuid)
        # print("return dset", ret)
        if ret is None:
            raise HTTPException(status_code=404, detail="Dataset not found")
        return ret


@router.get("/groups/{uuid}/links/{name}")
def link(req: Request, uuid: H5UUID, name: str) -> dict[Literal["link"], H5Link]:
    data, lock = req.app.state.get_data()
    with lock:
        obj, path = _uuid_to_obj(data, uuid)
        path.append(name)
        if name in obj:
            obj = obj[name]
            key: Literal["link"] = "link"
            ret = {key: _get_group_link(obj, path)}
            logger.debug("link name is %s", ret)
            return ret
    raise HTTPException(status_code=404, detail="Link not found")


@router.get("/groups/{uuid}/links")
def links(req: Request, uuid: H5UUID) -> dict[Literal["links"], list[H5Link]]:
    data, lock = req.app.state.get_data()
    with lock:
        obj, path = _uuid_to_obj(data, uuid)
        key: Literal["links"] = "links"
        ret = {key: _get_group_links(obj, path)}
        logger.debug("group links %s", ret)
        return ret


@router.get("/{typ}/{uuid}/attributes/{name}")
def attribute(
    req: Request, typ: Literal["groups", "datasets"], uuid: H5UUID, name: str
) -> H5ValuedAttribute:
    # print("get attr with name", typ, uuid, name)
    data, lock = req.app.state.get_data()
    with lock:
        obj, path = _uuid_to_obj(data, uuid)
        logger.debug(
            "start listing attributes typ %s id %s, obj %s, path: %s",
            typ,
            uuid,
            obj,
            path,
        )
        allattrs = _get_obj_attrs(data, path, include_values=True)
        for attr in allattrs:
            if attr.name == name:
                logger.debug("return attribute %s", attr)
                if isinstance(attr, H5ValuedAttribute):
                    return attr
    raise HTTPException(status_code=404, detail="Attribute not found")


@router.get("/{typ}/{uuid}/attributes")
def attributes(
    req: Request, typ: Literal["groups", "datasets"], uuid: H5UUID
) -> dict[Literal["attributes"], list[H5Attribute]]:
    data, lock = req.app.state.get_data()
    with lock:
        obj, path = _uuid_to_obj(data, uuid)
        logger.debug(
            "start listing attributes typ %s id %s, obj %s, path: %s",
            typ,
            uuid,
            obj,
            path,
        )
        # by calling _get_obj_attrs with False, it should never return H5ValuedAttribute
        attrs: list[H5Attribute] = _get_obj_attrs(data, path, include_values=False)  # type: ignore[assignment]
        return {"attributes": attrs}


@router.get("/groups/{uuid}")
def group(req: Request, uuid: H5UUID) -> H5Group:
    data, lock = req.app.state.get_data()
    with lock:
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
                root=_path_to_uuid([], collection_type="g-"),
                id=uuid,
                linkCount=linkCount,
                attributeCount=len(_get_obj_attrs(data, path)),
            )
            logger.debug("group is %s", group)
            return group
    raise HTTPException(status_code=404, detail="Group not found")


@router.get("/")
def read_root(req: Request) -> H5Root:
    logging.debug("data %s", req.app.state.get_data()[0])
    data, _ = req.app.state.get_data()
    if isinstance(data, dict):
        uuid = _path_to_uuid([], collection_type="g-")
        ret = H5Root(root=uuid)
        return ret
    raise Exception("data object is not a dict")


app = FastAPI()

app.include_router(router)


def get_data() -> Tuple[dict[str, Any], ContextManager]:
    dt = np.dtype({"names": ["a", "b"], "formats": [float, int]})

    arr = np.array([(0.5, 1)], dtype=dt)

    changing = np.ones((5, int(time.time()) % 10 + 5))
    data = {
        "map": {
            "x": [
                np.float64(-14.96431272),
                np.float64(-14.76401095),
                np.float64(-14.56276384),
                np.float64(-14.361908),
                np.float64(-14.16112703),
            ],
            "y": [
                np.float64(-10.00637736),
                np.float64(-10.00502708),
                np.float64(-10.00403313),
                np.float64(-10.00349819),
                np.float64(-10.00320074),
            ],
            "values": [
                np.float32(0.6831444),
                np.float32(0.0),
                np.float32(0.039953336),
                np.float32(0.14946304),
                np.float32(0.0),
            ],
        },
        "live": 34,
        "other": {"third": [1, 2, 3]},  # only _attrs allowed in root
        "other_attrs": {"NX_class": "NXother"},
        "image": np.ones((1000, 1000)),
        "image_attrs": {"listattr": [42, 43, 44, 45]},
        "changing_shape": changing,
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
    return data, nullcontext()


app.state.get_data = get_data
