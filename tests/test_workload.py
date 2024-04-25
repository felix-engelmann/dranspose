import logging
from typing import Awaitable, Callable, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq


@pytest.mark.asyncio
async def test_debugger(
    workload_generator: Callable[[Optional[int]], Awaitable[None]],
) -> None:
    await workload_generator()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5003/api/v1/status")
        state = await st.json()
        logging.info("gen state %s", state)

        st = await session.post(
            "http://localhost:5003/api/v1/open_socket",
            json={"type": zmq.PUB, "port": 4242},
        )
        state = await st.json()
        logging.info("open %s", state)

        st = await session.post("http://localhost:5003/api/v1/close_socket")
        state = await st.json()
        logging.info("close %s", state)
