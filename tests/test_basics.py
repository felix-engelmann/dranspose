import asyncio
import time

import pytest
import uvicorn

from dranspose.controller import app


@pytest.mark.asyncio
async def test_simple():
    await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_controller():
    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(4)
    server.should_exit = True
    await server_task
    time.sleep(0.1)
