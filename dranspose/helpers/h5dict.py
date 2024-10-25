import base64
import logging
import time
from typing import Any, Literal

import numpy as np
from fastapi import APIRouter, FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import Response

router = APIRouter()

# only activate for running it directly, otherwise it overwrites cli.py basic config
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def _get_now():
    return time.time()


def _path_to_uuid(path: list[str]) -> bytes:
    abspath = "/" + "/".join(path)
    print("abspath", abspath)
    uuid = base64.b16encode(abspath.encode())
    # print("uuid from function is", str(id(obj)).encode() + b"-" + uuid)
    return b"h5dict-" + uuid


def _get_obj_at_path(data, path):
    obj = data
    clean_path = []
    if isinstance(path, str):
        path = path.split("/")
    for p in path:
        print("path part is:", p)
        if p == "":
            continue
        print("traverse", obj)
        obj = obj[p]
        clean_path.append(p)
    return obj, clean_path


def _uuid_to_obj(data: dict[str, Any], uuid: str):
    logger.debug("parse %s", uuid)
    idstr, path = uuid.split("-")
    path = base64.b16decode(path).decode()
    logger.debug("raw path %s", path)
    # assert id(data) == int(idstr)
    return _get_obj_at_path(data, path)


def _make_shape_type(obj):
    extra = None
    if type(obj) is int:
        extra = {
            "shape": {"class": "H5S_SCALAR"},
            "type": {"base": "H5T_STD_I64LE", "class": "H5T_INTEGER"},
        }
    elif type(obj) is float:
        extra = {
            "shape": {"class": "H5S_SCALAR"},
            "type": {"base": "H5T_IEEE_F64LE", "class": "H5T_FLOAT"},
        }

    elif type(obj) is str:
        extra = {
            "type": {
                "class": "H5T_STRING",
                "length": "H5T_VARIABLE",
                "charSet": "H5T_CSET_UTF8",
                "strPad": "H5T_STR_NULLTERM",
            },
            "shape": {"class": "H5S_SCALAR"},
        }

    elif isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], str):
        extra = {
            "type": {
                "class": "H5T_STRING",
                "length": "H5T_VARIABLE",
                "charSet": "H5T_CSET_UTF8",
                "strPad": "H5T_STR_NULLTERM",
            },
            "shape": {"class": "H5S_SIMPLE", "dims": [len(obj)]},
        }
    else:
        try:
            arr = np.array(obj)
            extra = {
                "shape": {"class": "H5S_SIMPLE", "dims": arr.shape},
            }

            canonical = arr.dtype.descr[0][1]
            if canonical.startswith(">"):
                order = "BE"
            else:
                order = "LE"
            bytelen = int(canonical[2:])
            if canonical[1] == "f":
                # floating
                htyp = {"class": "H5T_FLOAT"}
                htyp["base"] = f"H5T_IEEE_F{8 * bytelen}{order}"
            elif canonical[1] in ["u", "i"]:
                htyp = {"class": "H5T_INTEGER"}
                signed = canonical[1].upper()
                htyp["base"] = f"H5T_STD_{signed}{8 * bytelen}{order}"
            else:
                logger.error("numpy type %s not available", arr.dtype)
            logging.debug("convert np dtype %s  to %s", arr.dtype, htyp)
            extra["type"] = htyp

        except Exception:
            pass
    return extra


def _dataset_from_obj(data, path, obj, uuid):
    ret = {
        "id": uuid,
        "attributeCount": len(_get_obj_attrs(data, path)),
        "lastModified": _get_now(),
        "created": _get_now(),
        "creationProperties": {
            "allocTime": "H5D_ALLOC_TIME_LATE",
            "fillTime": "H5D_FILL_TIME_IFSET",
            "layout": {"class": "H5D_CONTIGUOUS"},
        },
    }
    shape_type = _make_shape_type(obj)
    ret.update(shape_type)
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
    print("return dset", ret)
    return ret


def _get_group_link(obj, path):
    if isinstance(obj, dict):
        print("path to sub is: ", path)
        link = {
            "class": "H5L_TYPE_HARD",
            "collection": "groups",
            "id": _path_to_uuid(path),
            "title": path[-1],
            "created": _get_now(),
        }
    else:
        link = {
            "class": "H5L_TYPE_HARD",
            "collection": "datasets",
            "id": _path_to_uuid(path),
            "title": path[-1],
            "created": _get_now(),
        }
    return link


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


def _get_attr(aobj, name, include_values=True):
    print("get attribute")
    if name in aobj:
        extra = _make_shape_type(aobj[name])
        ret = {"name": name, "created": _get_now(), "lastModified": _get_now(), **extra}
        if include_values:
            val = aobj[name]
            if isinstance(val, np.ndarray):
                val = val.tolist()
            ret["value"] = val
        return ret


def _make_attrs(aobj, include_values=False):
    print("make attributes of ", aobj)
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
        print("normal attr fetch")
        parent, _ = _get_obj_at_path(data, path[:-1])
        print("parent is", parent)
        if f"{path[-1]}_attrs" in parent:
            print(
                "make attrs for",
            )
            return _make_attrs(
                parent[f"{path[-1]}_attrs"], include_values=include_values
            )
    return []


@router.get("/{typ}/{uuid}/attributes/{name}")
def attribute(req: Request, typ: Literal["groups", "datasets"], uuid: str, name: str):
    print("get attr with name", typ, uuid, name)
    data = req.app.state.get_data()
    obj, path = _uuid_to_obj(data, uuid)
    logger.debug(
        "start listing attributes typ %s id %s, obj %s, path: %s", typ, uuid, obj, path
    )
    allattrs = _get_obj_attrs(data, path, include_values=True)
    for attr in allattrs:
        if attr["name"] == name:
            logger.info("return attribute %s", attr)
            return attr
    raise HTTPException(status_code=404, detail="Attribute not found")


@router.get("/{typ}/{uuid}/attributes")
def attributes(req: Request, typ: Literal["groups", "datasets"], uuid: str):
    print("get attrs", typ, uuid)
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
        group = {
            "id": uuid,
            "root": _path_to_uuid([]),
            "linkCount": len(
                list(filter(lambda x: not x.endswith("_attrs"), obj.keys()))
            ),
            "attributeCount": len(_get_obj_attrs(data, path)),
            "lastModified": _get_now(),
            "created": _get_now(),
            "domain": "/",
        }
        logger.debug("group is %s", group)
        return group


@router.get("/")
def read_root(request: Request):
    logging.info("data %s", request.app.state.get_data())
    data = request.app.state.get_data()
    if isinstance(data, dict):
        uuid = _path_to_uuid([])
        ret = {
            "root": uuid,
            "created": time.time(),
            "owner": "admin",
            "class": "domain",
            "lastModified": time.time(),
        }
        return ret


app = FastAPI()

app.include_router(router)


def get_data():
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
