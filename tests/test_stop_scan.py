from typing import Awaitable, Callable, Optional
import zmq.asyncio

import aiohttp
import pytest
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
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.asyncio
async def test_stop_scans(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
) -> None:
    await reducer("examples.dummy.reducer:FluorescenceReducer")
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
    ntrig = 30
    await set_uniform_sequence({StreamName("eiger")}, ntrig)
    context = zmq.asyncio.Context()
    async with aiohttp.ClientSession() as session:
        await session.post("http://localhost:5000/api/v1/stop")

    await wait_for_finish()

    context.destroy()
