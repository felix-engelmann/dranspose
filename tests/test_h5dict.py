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

    app.state.publish = {
        "live": 34,
        "other": {"third": [1, 2, 3]},
        "image": np.ones((1000, 1000)),
        "hello": "World",
        "_attrs": {"NX_class": "NXentry"},
    }

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

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    server.should_exit = True
    await server_task

    await asyncio.sleep(0.5)
