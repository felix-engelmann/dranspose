import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional


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
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.skipif(
    "config.getoption('rust')", reason="rust does not allow deep access"
)
@pytest.mark.asyncio
async def test_restart_ingester(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    ingester = await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("eiger")})
    ntrig = 10
    await set_uniform_sequence({StreamName("eiger")}, ntrig)

    with zmq.asyncio.Context() as context:
        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))
        content = await wait_for_finish()
        await asyncio.sleep(1)
        await create_ingester(
            ZmqPullSingleIngester(
                settings=ZmqPullSingleSettings(
                    ingester_streams=[StreamName("eiger")],
                    upstream_url=Url("tcp://localhost:9999"),
                    ingester_url=Url("tcp://localhost:10011"),
                ),
            )
        )
        await ingester.close()
        await asyncio.sleep(1)

    print(content)
