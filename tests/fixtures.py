import asyncio

import pytest_asyncio
import uvicorn

from dranspose.controller import app


@pytest_asyncio.fixture()
async def controller():
    config = uvicorn.Config(app, port=5000, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)
    yield
    server.should_exit = True
    await server_task
    await asyncio.sleep(0.1)
