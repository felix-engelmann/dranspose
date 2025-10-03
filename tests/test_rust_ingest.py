import asyncio
import logging
from typing import Callable, Any, Coroutine, Optional, Awaitable

import aiohttp
import pytest
import zmq.asyncio

from dranspose.protocol import (
    WorkerName,
    StreamName,
)
from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish, vworker, monopart_sequence

rust = pytest.mark.skipif("not config.getoption('rust')")


@pytest.mark.skip
@pytest.mark.asyncio
async def test_rust_basic(
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    stream_small: Callable[
        [zmq.Context[Any], int, int, Optional[float]], Coroutine[Any, Any, None]
    ],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w2"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w3"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w4"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w5"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w6"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w7"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w8"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w9"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w10"), threaded=True)  # type: ignore[call-arg]

    await asyncio.sleep(2)

    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
        logging.info("startup done")

        ntrig = 50000
        sequence = monopart_sequence(
            {
                "eiger": [[vworker(i // 10)] for i in range(1, ntrig)],
            }
        )
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence", json=sequence
        )
        assert resp.status == 200

    context = zmq.asyncio.Context()
    await stream_small(context, 9999, ntrig - 1, 0.000001)

    await wait_for_finish()

    context.destroy()
