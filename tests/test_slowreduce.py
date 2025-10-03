import asyncio
import logging

import h5pyd
from typing import Awaitable, Callable, Any, Coroutine, Optional

from dranspose.protocol import (
    StreamName,
    WorkerName,
)
from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_finish, wait_for_controller, set_uniform_sequence


@pytest.mark.asyncio
async def test_slowreduce(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName | Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[
        [zmq.Context[Any], int, int, float], Coroutine[Any, Any, None]
    ],
) -> None:
    await reducer("examples.slow.reducer:SlowReducer")
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.slow.worker:SlowWorker",
            ),
        )
    )
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w2"),
                worker_class="examples.slow.worker:SlowWorker",
            ),
        )
    )
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
        st = await session.get(
            "http://localhost:5000/api/v1/load?intervals=1&intervals=10&scan=True"
        )
        load = await st.json()
        assert load == {}

    ntrig = 4
    await set_uniform_sequence({StreamName("eiger")}, ntrig)
    with zmq.asyncio.Context() as context:
        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1, 0.1))
        await wait_for_finish()

    def work() -> None:
        f = h5pyd.File("http://localhost:5001/", "r")
        logging.info("file %s", list(f["map"].keys()))
        assert len(list(f["map"].keys())) == ntrig + 1
        for evn in range(ntrig + 1):
            assert f["map"][str(evn)][()] == 1

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)
