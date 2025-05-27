import asyncio
import json
import logging
import subprocess
import aiohttp

import pytest

from dranspose.helpers.utils import cancel_and_wait


async def sink(msgs):
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        async with session.get("http://localhost:5000/api/v1/log_stream") as r:
            async for line in r.content:
                msgs.append(json.loads(line))


@pytest.mark.asyncio
async def test_stream() -> None:
    p = await asyncio.create_subprocess_exec(
        "dranspose", "controller", stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    logging.info("started process")
    await asyncio.sleep(4)
    msgs = []
    s = asyncio.create_task(sink(msgs))
    logging.info("created sink")
    await asyncio.sleep(1)
    wo = await asyncio.create_subprocess_exec(
        "dranspose", "worker", stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    logging.info("created worker")
    await asyncio.sleep(4)
    wo.terminate()
    p.terminate()
    logging.info("terminated")
    await cancel_and_wait(s)
    allmsg = [m["msg"] for m in msgs]
    assert "started work task" in allmsg
    assert "cannot get reducer configuration" in allmsg
    assert "registered ready message" in allmsg
    assert "work thread finished" in allmsg
    assert "clean up in sockets" in allmsg
    assert "start new active map reserved_start_end starting at 0" in allmsg
