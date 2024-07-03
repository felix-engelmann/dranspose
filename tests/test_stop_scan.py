import asyncio
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
    EnsembleState,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.worker import Worker


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

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"eiger"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        ntrig = 30
        mp = {
            "eiger": [
                [VirtualWorker(constraint=VirtualConstraint(i)).model_dump(mode="json")]
                for i in range(ntrig)
            ],
        }
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json=mp,
        )
        assert resp.status == 200
        await resp.json()

    context = zmq.asyncio.Context()

    # asyncio.create_task()

    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5000/api/v1/stop",
        )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while content["finished"] is False:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    context.destroy()
