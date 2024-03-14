import asyncio
import logging
from typing import Callable, Any, Coroutine

import pytest
import zmq.asyncio

from dranspose.helpers.utils import cancel_and_wait


@pytest.mark.asyncio
async def test_zmq_cancel(
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]]
) -> None:
    async def consume() -> None:
        c = zmq.asyncio.Context()
        s = c.socket(zmq.PULL)
        s.connect("tcp://localhost:9999")
        for i in range(13):
            logging.info("recv %d", i)
            task = s.poll()
            waiting = asyncio.gather(*[task])
            if i == 6:
                await cancel_and_wait(waiting)
                continue
            done = await waiting
            logging.info("done %s", done)
            data = s.recv_multipart(copy=False)
            logging.info("i %d, data %s", i, data)
        c.destroy()

    ctask = asyncio.create_task(consume())

    context = zmq.asyncio.Context()
    await stream_eiger(context, 9999, 4)
    await asyncio.sleep(1)
    await stream_eiger(context, 9999, 4)
    await ctask

    context.destroy()
