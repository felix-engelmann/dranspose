import asyncio
import logging
import os
import time

import pytest


import redis.asyncio as redis


async def echo():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, protocol=3)

    last = 0
    run = True
    while run:
        data = await r.xread({"dranspose:assign-stream": last}, block=1000)
        logging.debug("echo received %s", data)
        for par in data.get("dranspose:assign-stream", [[]])[0]:
            logging.debug(par[1]["assign"])
            await r.xadd("dranspose:ready-stream", {"ready": par[1]["assign"]})
            last = par[0]
            if par[1]["assign"] == "stop":
                logging.info("received stop, break")
                run = False


@pytest.mark.asyncio
async def test_rate() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, protocol=3)

    await r.delete("dranspose:ready-stream")
    await r.delete("dranspose:assign-stream")

    task = asyncio.create_task(echo())

    last = 0
    num = 3000
    start = time.perf_counter()
    for i in range(num):
        await r.xadd("dranspose:assign-stream", {"assign": i})
        ready = await r.xread({"dranspose:ready-stream": last}, block=1000)
        logging.debug("received %s", ready)
        assert ready["dranspose:ready-stream"][0][0][1]["ready"] == str(i)
        last = ready["dranspose:ready-stream"][0][0][0]

    elapsed = time.perf_counter() - start

    logging.info(
        "echoing %d rounds took %f seconds, %f Hz", num, elapsed, num / elapsed
    )

    await r.xadd("dranspose:assign-stream", {"assign": "stop"})

    await task

    await r.delete("dranspose:ready-stream")
    await r.delete("dranspose:assign-stream")

    await r.aclose()
