import asyncio
import logging
from contextlib import asynccontextmanager
import random
from typing import AsyncGenerator

from fastapi import FastAPI, Request

import aiohttp

import pytest
import uvicorn


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    if hasattr(app.state, "identity"):
        logging.warning("app state before startup %d", app.state.identity)
    app.state.identity = random.randint(0, 10000000)
    logging.warning("app state after set %d", app.state.identity)
    yield


def create_app():
    app = FastAPI(lifespan=lifespan)

    @app.get("/identity")
    async def get_id(request: Request) -> int:
        return request.app.state.identity

    return app


@pytest.mark.asyncio
async def test_multi() -> None:
    app1 = create_app()
    config = uvicorn.Config(app1, port=5000, log_level="debug")
    server = uvicorn.Server(config=config)
    task = asyncio.create_task(server.serve())

    await asyncio.sleep(1)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/identity")
        identity1before = await st.read()
        logging.info("id of first is %s", identity1before)

    app2 = create_app()
    config2 = uvicorn.Config(app2, port=5001, log_level="debug")
    server2 = uvicorn.Server(config=config2)
    task2 = asyncio.create_task(server2.serve())

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/identity")
        identity1after = await st.read()
        logging.info("id of first after second started is %s", identity1after)

    assert identity1before == identity1after

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5001/identity")
        identity = await st.read()
        logging.info("id of second is %s", identity)

    assert identity1after != identity

    server.should_exit = True
    server2.should_exit = True

    await task
    await task2
