import asyncio
import logging
import multiprocessing
from typing import Any

import aiohttp
import pytest
import redis.asyncio as redis
import uvicorn
from dranspose.protocol import (
    EnsembleState,
    RedisKeys,
)


class UvicornServer(multiprocessing.Process):
    def __init__(self, config: uvicorn.Config):
        super().__init__()
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self) -> None:
        self.terminate()

    def run(self, *args: Any, **kwargs: Any) -> None:
        self.server.run()


# A lot of distributed state assumes that there is only ever one controller with the same prefix running per redis instance
# This does not hold when redeploying on k8s as it starts the new container before the old is terminated.
# The controller now implements a lock which delays the start of a controller if another is still holding it and wait to properly finish.
# An artifact of interleaved stopping and starting was, that the parameters created by the new controller were cleaned up by the closing of the old one and then led to inconsistent parameters.
# In addition to the locking of the controller, the consistent_parameters routine assures that all parameters with values are present in redis.


@pytest.mark.asyncio
async def test_restart() -> None:
    config = uvicorn.Config(
        "dranspose.controller:app", host="127.0.0.1", port=5000, log_level="debug"
    )
    instance = UvicornServer(config=config)
    instance.start()

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

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

        config2 = uvicorn.Config(
            "dranspose.controller:app", host="127.0.0.1", port=5001, log_level="debug"
        )
        instance2 = UvicornServer(config=config2)
        instance2.start()

        instance.stop()

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

    instance2.stop()

    await r.aclose()
