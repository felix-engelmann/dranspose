import asyncio
from contextlib import nullcontext
import logging
from typing import Any, ContextManager, Tuple

import aiohttp
import numpy as np
import pytest
import uvicorn
from fastapi import FastAPI
from dranspose.helpers.h5dict import router
from readerwriterlock.rwlock import RWLockFair

import h5pyd


@pytest.mark.asyncio
async def test_mapping() -> None:
    app = FastAPI()

    app.include_router(router, prefix="/results")

    def get_data() -> Tuple[dict[str, Any], ContextManager]:
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
            "control": {},
            "azint": {"data": []},
        }
        return data, nullcontext()

    app.state.get_data = get_data

    config = uvicorn.Config(app, port=5000, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/results")
        data = await st.json()
        logging.info("content %s", data)

    def work() -> None:
        f = h5pyd.File("/", "r", endpoint="http://localhost:5000/results")
        logging.info("file %s", list(f.keys()))
        logging.info("typ %s", f["map"])
        logging.info("comp %s", f["map/values"])
        logging.info("comp data %s", f["map/values"][:])
        assert f["map/values"].dtype == np.float32
        assert f["map/x"].dtype == np.float64

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    server.should_exit = True
    await server_task

    await asyncio.sleep(0.5)


@pytest.mark.asyncio
@pytest.mark.do_not_fail_on_err_log
async def test_lock() -> None:
    app = FastAPI()

    app.include_router(router, prefix="/results")

    rw_lock = RWLockFair()
    r_lock = rw_lock.gen_rlock()
    w_lock = rw_lock.gen_wlock()
    # lock = Lock()

    def get_data() -> Tuple[dict[str, Any], ContextManager[Any]]:
        data: dict[str, Any] = {
            "live": 34,
            "other": {"third": [1, 2, 3]},  # only _attrs allowed in root
            "other_attrs": {"NX_class": "NXother"},
            "spaced group": {"space ds": 4, "space ds_attrs": {"bla": 5}},
            "spaced group_attrs": {"spaced attr": 3},
            "image": np.ones((1000, 1000)),
            "specialtyp": np.ones((10, 10), dtype=">u8"),
            "specialtyp_attrs": {"NXdata": "NXspecial"},
            "hello": "World",
            "_attrs": {"NX_class": "NXentry"},
        }

        return data, r_lock  # type: ignore

    app.state.get_data = get_data

    config = uvicorn.Config(app, port=5000, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/results")
        data = await st.json()
        logging.info("content %s", data)

        comr_data = await session.get(
            "http://localhost:5000/results/datasets/d-h5dict-2F696D616765/value",
            headers={"Accept-Encoding": "deflate"},
        )
        data_len = comr_data.content_length
        logging.info("data len %d", data_len)
        logging.info("headers are %s", comr_data.headers.items())
        assert "Content-Encoding" not in comr_data.headers
        assert data_len == 8e6

        with w_lock:
            with pytest.raises(TimeoutError):
                comr_data = await session.get(
                    "http://localhost:5000/results/datasets/h5dict-2F696D616765/value",
                    headers={"Accept-Encoding": "deflate"},
                    timeout=aiohttp.ClientTimeout(1),
                )

        w_lock.acquire()
        t = asyncio.create_task(
            session.get(
                "http://localhost:5000/results/datasets/h5dict-2F696D616765/value",
                headers={"Accept-Encoding": "deflate"},
            )
        )
        await asyncio.sleep(0.1)
        assert not t.done()
        w_lock.release()
        await asyncio.sleep(0.1)
        assert t.done()

    def work() -> None:
        f = h5pyd.File(
            "/", "r", endpoint="http://localhost:5000/results", timeout=1, retries=0
        )
        w_lock.acquire()

        with pytest.raises(KeyError):
            f["live"]
        w_lock.release()

        logging.info("file %s", f["live"][()])
        logging.info("typ %s", f["specialtyp"])
        # logging.info("comp %s", f["composite"])
        # logging.info("comp data %s", f["composite"][:])
        # logging.info("spaces %s", list(f["spaced group"].keys()))
        # assert list(f["spaced group"].keys()) == ["space ds"]
        # assert f["spaced group"].attrs["spaced attr"] == 3
        # assert f["spaced group/space ds"][()] == 4
        # assert f["spaced group/space ds"].attrs["bla"] == 5
        # assert f["specialtyp"].dtype == ">u8"
        # assert f["specialtyp"].attrs["NXdata"] == "NXspecial"
        # assert f["other"].attrs["NX_class"] == "NXother"
        # assert f.attrs["NX_class"] == "NXentry"
        # assert list(f["composite"].attrs["axes"]) == ["I", "q"]
        # assert f["composite"].dtype == np.dtype(
        #     {"names": ["a", "b"], "formats": [float, int]}
        # )
        # assert np.array_equal(f["image"], np.ones((1000, 1000)))

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    server.should_exit = True
    await server_task

    await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_root() -> None:
    app = FastAPI()

    app.include_router(router, prefix="/results")

    def get_data() -> Tuple[dict[str, Any], ContextManager]:
        dt = np.dtype({"names": ["a", "b"], "formats": [float, int]})

        arr = np.array([(0.5, 1)], dtype=dt)

        data = {
            "live": 34,
            "other": {"third": [1, 2, 3]},  # only _attrs allowed in root
            "other_attrs": {"NX_class": "NXother"},
            "spaced group": {"space ds": 4, "space ds_attrs": {"bla": 5}},
            "spaced group_attrs": {"spaced attr": 3},
            "image": np.ones((1000, 1000)),
            "specialtyp": np.ones((10, 10), dtype=">u8"),
            "specialtyp_attrs": {"NXdata": "NXspecial"},
            "composite": arr,
            "composite_attrs": {"axes": ["I", "q"]},
            "hello": "World",
            "_attrs": {"NX_class": "NXentry"},
        }
        return data, nullcontext()

    app.state.get_data = get_data

    config = uvicorn.Config(app, port=5000, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/results")
        data = await st.json()
        logging.info("content %s", data)

        comr_data = await session.get(
            "http://localhost:5000/results/datasets/d-h5dict-2F696D616765/value"
        )
        data_len = comr_data.content_length
        logging.info("data len %d", data_len)
        logging.info("headers are %s", comr_data.headers.items())
        assert comr_data.headers["Content-Encoding"] == "gzip"
        assert data_len == 11683

    async with aiohttp.ClientSession() as session:
        comr_data = await session.get(
            "http://localhost:5000/results/datasets/d-h5dict-2F696D616765/value",
            headers={"Accept-Encoding": "deflate"},
        )
        data_len = comr_data.content_length
        logging.info("data len %d", data_len)
        logging.info("headers are %s", comr_data.headers.items())
        assert "Content-Encoding" not in comr_data.headers
        assert data_len == 8e6

    def work() -> None:
        f = h5pyd.File("/", "r", endpoint="http://localhost:5000/results")
        logging.info("file %s", f["live"][()])
        logging.info("typ %s", f["specialtyp"])
        logging.info("comp %s", f["composite"])
        logging.info("comp data %s", f["composite"][:])
        logging.info("spaces %s", list(f["spaced group"].keys()))
        assert list(f["spaced group"].keys()) == ["space ds"]
        assert f["spaced group"].attrs["spaced attr"] == 3
        assert f["spaced group/space ds"][()] == 4
        assert f["spaced group/space ds"].attrs["bla"] == 5
        assert f["specialtyp"].dtype == ">u8"
        assert f["specialtyp"].attrs["NXdata"] == "NXspecial"
        assert f["other"].attrs["NX_class"] == "NXother"
        assert f.attrs["NX_class"] == "NXentry"
        assert list(f["composite"].attrs["axes"]) == ["I", "q"]
        assert f["composite"].dtype == np.dtype(
            {"names": ["a", "b"], "formats": [float, int]}
        )
        assert np.array_equal(f["image"], np.ones((1000, 1000)))

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    server.should_exit = True
    await server_task

    await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_slice() -> None:
    app = FastAPI()

    app.include_router(router, prefix="/results")

    def get_data() -> dict[str, Any]:
        return {
            "image": [[r * 100 + c for c in range(10)] for r in range(10)],
        }, nullcontext()

    app.state.get_data = get_data

    config = uvicorn.Config(app, port=5000, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)

    async with aiohttp.ClientSession() as session:
        comr_data = await session.get(
            "http://localhost:5000/results/datasets/d-h5dict-2F696D616765/value?select=[2:5,7:20]",
            headers={"Accept-Encoding": "deflate"},
        )
        orig = np.array([[r * 100 + c for c in range(10)] for r in range(10)])
        logging.info("original %s", orig.dtype)
        raw = await comr_data.content.read()
        arr = np.frombuffer(raw, dtype=np.int64)
        assert np.array_equal(orig[2:5, 7:20].reshape(-1), arr)
        logging.info("got data %s", arr)

    server.should_exit = True
    await server_task

    await asyncio.sleep(0.5)


def test_dtype_to_h5() -> None:
    numpytypes = {
        "u1",
        ">u2",
        ">u4",
        ">u8",
        "u1",
        "<u2",
        "<u4",
        "<u8",
        "i1",
        ">i2",
        ">i4",
        ">i8",
        "<i2",
        "<i4",
        "<i8",
        # ">f2",
        ">f4",
        ">f8",
        # ">f16",
        # "<f2",
        "<f4",
        "<f8",
        # "<f16"
    }
    for typ in numpytypes:
        dtype = np.dtype(typ)
        # {"base": "H5T_IEEE_F64LE", "class": "H5T_FLOAT"}
        # {"base": "H5T_STD_I64LE", "class": "H5T_INTEGER"}
        canonical = dtype.descr[0][1]
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
            raise NotImplementedError()
        logging.info("%s %s", typ, htyp)
        logging.info("%s %s, %s", typ, dtype, canonical)
