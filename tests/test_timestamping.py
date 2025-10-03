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
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.asyncio
async def test_timestamps(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName | Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[
        [zmq.Context[Any], int, int, float], Coroutine[Any, Any, None]
    ],
) -> None:
    await reducer("examples.timing.reducer:TimingReducer")
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.timing.worker:TimingWorker",
            ),
        )
    )
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("fast")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("fast")})
    async with aiohttp.ClientSession() as session:
        st = await session.get(
            "http://localhost:5000/api/v1/load?intervals=1&intervals=10&scan=True"
        )
        load = await st.json()
        assert load == {}

    ntrig = 4
    await set_uniform_sequence({StreamName("fast")}, ntrig)
    with zmq.asyncio.Context() as context:
        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1, 0.1))
        await wait_for_finish()

    def work() -> None:
        f = h5pyd.File("http://localhost:5001/", "r")
        logging.info("fast %s", f["fast"][:])
        data = f["fast"][:]
        assert len(data) == ntrig - 1
        for t in data:
            assert len(t) == 2
            assert t[0] < t[1]
            if t[1] > 0.1:
                logging.warning(
                    "the pipeline end to end latency is larger then 100ms: %s", t
                )

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)
