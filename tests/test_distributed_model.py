import asyncio
import logging
import os
import redis.asyncio as redis

import pytest
from pydantic import BaseModel

from dranspose.helpers.utils import cancel_and_wait
from dranspose.parameters import DistributedModel
from dranspose.protocol import RedisKeys


class PayloadParameters(BaseModel):
    roi1: str = "bla"
    file_parameter: str = ""
    file_blob: bytes = b"\x00"
    number: int = 0
    flow: float = 0.0
    isit: bool = False
    complex: tuple[int, int] | None = None
    crazy: dict[str, dict[int, str]] = {}


class PayloadParametersSubset(BaseModel):
    roi1: str = "bla"
    file_parameter: str = ""


@pytest.mark.asyncio
async def test_base():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, protocol=3)

    pub = DistributedModel(PayloadParameters, r)
    sub = DistributedModel(PayloadParameters, r)

    task = asyncio.create_task(sub.subscribe())

    await asyncio.sleep(0.3)
    logging.info("data %s", pub.data)
    logging.info("data %s", pub.state)
    await pub.patch({"roi1": "new_value", "crazy": {"bla": {"1": "0"}}})
    logging.info("data %s", pub.data)

    await asyncio.sleep(1)

    logging.info("sub %s", sub.data)
    logging.info("sub %s", sub.state)

    assert sub.data == pub.data

    assert pub.is_consistent([sub.state])

    late = DistributedModel(PayloadParameters, r)
    task_late = asyncio.create_task(late.subscribe())
    await asyncio.sleep(0.5)

    await pub.patch({"roi1": "for late"})

    await asyncio.sleep(1)

    logging.info("late %s", late.data)
    logging.info("late %s", late.state)

    assert late.data.roi1 == "for late"

    assert pub.is_consistent([sub.state, late.state]) is False

    await pub.refresh()
    await asyncio.sleep(1)

    assert late.data == pub.data
    assert pub.is_consistent([sub.state, late.state])

    await cancel_and_wait(task)
    await cancel_and_wait(task_late)

    await r.delete(RedisKeys.parameter_updates())
    await r.aclose()


@pytest.mark.asyncio
async def test_different():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, protocol=3)

    pub = DistributedModel(PayloadParameters, r)
    sub = DistributedModel(PayloadParametersSubset, r)

    task = asyncio.create_task(sub.subscribe())
    await asyncio.sleep(0.3)

    await pub.patch({"roi1": "new_value", "crazy": {"bla": {"1": "0"}}})
    await asyncio.sleep(0.3)

    logging.info("sub %s", sub.data)
    logging.info("sub %s", sub.state)

    assert sub.data.roi1 == "new_value"

    assert pub.is_consistent([sub.state]) is False

    await cancel_and_wait(task)

    await r.delete(RedisKeys.parameter_updates())
    await r.aclose()
