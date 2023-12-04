import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Never, Optional
from dranspose.protocol import (
    EnsembleState,
    RedisKeys,
    StreamName,
    WorkerName,
    WorkerTimes,
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
import pytest
import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.worker import Worker
from tests.fixtures import (
    controller,
    reducer,
    create_worker,
    create_ingester,
    stream_eiger,
    stream_orca,
    stream_alba,
)


@pytest.mark.asyncio
async def test_simple(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[
        [zmq.Context[Any], int, int, float], Coroutine[Any, Any, Never]
    ],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_ingester(
        StreamingSingleIngester(
            name=StreamName("eiger"),
            settings=StreamingSingleSettings(upstream_url=Url("tcp://localhost:9999")),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"eiger"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "eiger": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(2 * i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(1, ntrig)
                ],
            },
        )
        assert resp.status == 200
        uuid = await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1, 0.1))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    context.destroy()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/status")
        content = await st.json()
        assert (
            len([WorkerTimes.model_validate(x) for x in content["processing_times"]])
            == ntrig + 1
        )
