import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.protocol import (
    StreamName,
    WorkerName,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish, vworker, monopart_sequence

@pytest.mark.asyncio
async def test_simple(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence",
            json=monopart_sequence({
                "eiger": [[vworker(i)] for i in range(1, ntrig - 5)]
                + [None]
                + [[vworker(i)] for i in range(ntrig - 5, ntrig)],
            }),
        )
        assert resp.status == 200

    with zmq.asyncio.Context() as context:
        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))
        content = await wait_for_finish()

    print(content)
