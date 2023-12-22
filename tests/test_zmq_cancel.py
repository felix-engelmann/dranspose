import asyncio
import logging

import pytest
import zmq.asyncio


@pytest.mark.asyncio
async def test_zmq_cancel(stream_eiger):
    async def consume():
        c = zmq.asyncio.Context()
        s = c.socket(zmq.PULL)
        s.connect("tcp://localhost:9999")
        for i in range(13):
            logging.info("recv %d", i)
            task = s.poll()  # recv_multipart(copy=False)
            waiting = asyncio.wait([task], return_when=asyncio.ALL_COMPLETED)
            if i == 6:
                task.cancel()
                waiting.close()
                continue
            done, pend = await waiting
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
