import asyncio
import logging
import os

import pytest


import redis.asyncio as redis


@pytest.mark.asyncio
async def test_distributed() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, protocol=3)

    keys = await r.keys("dranspose:*")
    logging.info("keys %s", keys)

    key = await r.set("testkey", "didit", px=800, nx=True)
    logging.info("key set to %s", key)
    assert key is True
    key = await r.set("testkey", "didit", px=500, nx=True)
    logging.info("key set to %s", key)
    assert key is None

    ttl = await r.pttl("testkey")
    logging.info("ttl is %f", ttl)
    await asyncio.sleep(1)

    key = await r.set("testkey", "didit", px=500, nx=True)
    logging.info("key set to %s", key)
    assert key is True

    await r.aclose()
