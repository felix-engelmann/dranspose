import asyncio
import logging

import aiohttp
import numpy as np
import pytest
import uvicorn
from fastapi import FastAPI
from dranspose.helpers.h5dict import router

import h5pyd


@pytest.mark.asyncio
async def test_root():
    app = FastAPI()

    app.include_router(router, prefix="/results")

    def get_data():
        return {
            "live": 34,
            "other": {"third": [1, 2, 3]},
            "image": np.ones((1000, 1000)),
            "specialtyp": np.ones((10, 10), dtype=">u8"),
            "hello": "World",
            "_attrs": {"NX_class": "NXentry"},
        }

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

    def work():
        f = h5pyd.File("/", "r", endpoint="http://localhost:5000/results")
        logging.info("file %s", f["live"][()])
        logging.info("typ %s", f["specialtyp"])
        assert f["specialtyp"].dtype == ">u8"

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    server.should_exit = True
    await server_task

    await asyncio.sleep(0.5)


def test_dtype_to_h5():
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
            htyp["base"] = f"H5T_IEEE_F{8*bytelen}{order}"
        elif canonical[1] in ["u", "i"]:
            htyp = {"class": "H5T_INTEGER"}
            signed = canonical[1].upper()
            htyp["base"] = f"H5T_STD_{signed}{8 * bytelen}{order}"
        else:
            raise NotImplementedError()
        logging.info("%s %s", typ, htyp)
        logging.info("%s %s, %s", typ, dtype, canonical)
