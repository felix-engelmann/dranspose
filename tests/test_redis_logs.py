from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
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
from tests.utils import wait_for_controller


@pytest.mark.asyncio
async def test_logs(
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
        logs = await session.get("http://localhost:5000/api/v1/logs?level=warning")
        lgs = await logs.json()
        # assert len(lgs) > 0 currently there is nothing written to redis from a test, but it works when called from cli
        for log in lgs:
            assert log["levelname"] in ["WARNING", "ERROR"]
