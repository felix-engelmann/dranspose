import asyncio
import logging

import pytest


import redis.asyncio as redis


@pytest.mark.asyncio
async def test_distributed() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

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
