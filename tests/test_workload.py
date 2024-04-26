import asyncio
import logging
from typing import Awaitable, Callable, Optional, Any

import aiohttp

import pytest
import zmq.asyncio
import zmq


async def consume(
    ctx: zmq.Context[Any], num: int, typ: int = zmq.PULL, port: int = 9999
) -> int:
    s = ctx.socket(typ)
    logging.info("created socket")
    s.connect(f"tcp://127.0.0.1:{port}")
    if typ == zmq.SUB:
        s.setsockopt(zmq.SUBSCRIBE, b"")
        num -= 1
    logging.info("connected socket to port %s", port)
    for _ in range(num):
        data = await s.recv_multipart(copy=False)
        logging.info("received data %s", data)

    return num


@pytest.mark.parametrize("srv,cli", [(zmq.PUSH, zmq.PULL), (zmq.PUB, zmq.SUB)])
@pytest.mark.asyncio
async def test_debugger(
    workload_generator: Callable[[Optional[int]], Awaitable[None]], srv, cli
) -> None:
    await workload_generator(5003)

    ctx = zmq.asyncio.Context()
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5003/api/v1/status")
        state = await st.json()
        logging.info("gen state %s", state)

        task = asyncio.create_task(consume(ctx, 5, typ=cli))
        logging.info("created consumer task")

        st = await session.post(
            "http://localhost:5003/api/v1/open_socket",
            json={"type": srv, "port": 9999},
        )
        state = await st.json()
        logging.info("open %s", state)

        st = await session.post(
            "http://localhost:5003/api/v1/frames",
        )
        state = await st.json()
        logging.info("sending frames %s", state)

        st = await session.get("http://localhost:5003/api/v1/status")
        state = await st.json()
        while not state:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5003/api/v1/status")
            state = await st.json()

        st = await session.post("http://localhost:5003/api/v1/close_socket")
        state = await st.json()
        logging.info("close %s", state)

    await task
    ctx.destroy(linger=0)
