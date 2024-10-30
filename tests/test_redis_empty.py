import logging

import pytest


import redis.asyncio as redis


@pytest.mark.asyncio
async def test_distributed() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)
    raw_redis = redis.from_url("redis://localhost:6379/0?protocol=3")

    keys = await r.keys("dranspose:*")
    logging.info("keys %s", keys)

    key = await r.set("dranspose:testkey", b"")
    logging.info("key set to %s", key)
    assert key is True

    params = await raw_redis.get("dranspose:testkey")
    logging.info("param is %s", params)
    assert params is not None
    keys = await r.keys("dranspose:*")
    logging.info("keys %s", keys)

    await r.delete("dranspose:testkey")

    await r.aclose()
    await raw_redis.aclose()
