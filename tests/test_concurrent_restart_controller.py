import asyncio
import logging
import os

import aiohttp
import pytest
import redis.asyncio as redis
import uvicorn
from dranspose.protocol import (
    RedisKeys,
    EnsembleState,
)
from dranspose.controller import create_app

# A lot of distributed state assumes that there is only ever one controller with the same prefix running per redis instance
# This does not hold when redeploying on k8s as it starts the new container before the old is terminated.
# The controller now implements a lock which delays the start of a controller if another is still holding it and wait to properly finish.
# An artifact of interleaved stopping and starting was, that the parameters created by the new controller were cleaned up by the closing of the old one and then led to inconsistent parameters.
# In addition to the locking of the controller, the consistent_parameters routine assures that all parameters with values are present in redis.


@pytest.mark.asyncio
async def test_restart() -> None:
    og_app = create_app()
    og_config = uvicorn.Config(og_app, host="127.0.0.1", port=5000, log_level="debug")
    og_server = uvicorn.Server(config=og_config)
    og_task = asyncio.create_task(og_server.serve())

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, protocol=3)

    async with aiohttp.ClientSession() as session:
        running = False
        while not running:
            await asyncio.sleep(0.3)
            try:
                st = await session.get("http://localhost:5000/api/v1/config")
                running = True
            except aiohttp.client_exceptions.ClientConnectorError:
                pass

        state = EnsembleState.model_validate(await st.json())
        logging.info("ensemble state uuids are %s", state)

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/testname",
            data=b"testvalue",
        )
        assert resp.status == 200

        lock = await r.keys(RedisKeys.lock())
        logging.info("lock is %s", lock)

        params = await r.keys(RedisKeys.parameters("*", "*"))
        logging.info("redis keys are %s", params)

        assert len(params) == 1
        assert params[0].startswith("dranspose:parameters:testname:")

        new_app = create_app()
        new_config = uvicorn.Config(
            new_app, host="127.0.0.1", port=5001, log_level="debug"
        )
        new_server = uvicorn.Server(config=new_config)
        new_task = asyncio.create_task(new_server.serve())

        await asyncio.sleep(2)

        og_server.should_exit = True
        await og_task

        running = False
        while not running:
            await asyncio.sleep(0.3)
            try:
                st = await session.get("http://localhost:5001/api/v1/config")
                running = True
            except aiohttp.client_exceptions.ClientConnectorError:
                pass

        state = EnsembleState.model_validate(await st.json())
        logging.info("new controller state %s", state)

        resp = await session.post(
            "http://localhost:5001/api/v1/parameter/testnew",
            data=b"newvalue",
        )
        assert resp.status == 200

        params = await r.keys(RedisKeys.parameters("*", "*"))
        logging.info("redis keys are %s", params)

        assert any([p.startswith("dranspose:parameters:testnew") for p in params])

        running = True
        while running:
            await asyncio.sleep(0.3)
            try:
                await session.get("http://localhost:5000/api/v1/config")
            except aiohttp.client_exceptions.ClientConnectorError:
                running = False

    params = await r.keys(RedisKeys.parameters("*", "*"))
    logging.info("redis keys after old stop %s", params)

    assert any([p.startswith("dranspose:parameters:testnew") for p in params])

    new_server.should_exit = True

    await new_task

    keys = await r.keys("*:*")

    await r.aclose()

    assert keys == []
