import os
import pickle

import redis.asyncio as redis
import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Never, Optional
import zmq.asyncio

import aiohttp
import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_contrast import StreamingContrastIngester, StreamingContrastSettings
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
from dranspose.ingesters.streaming_xspress3 import StreamingXspressIngester, StreamingXspressSettings
from dranspose.protocol import EnsembleState, RedisKeys, StreamName, WorkerName
from dranspose.worker import Worker, WorkerSettings

from tests.fixtures import (
    controller,
    reducer,
    create_worker,
    create_ingester,
    stream_pkls,
)


@pytest.mark.asyncio
async def test_reduction(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pkls: Callable[[zmq.Context[Any], int, os.PathLike, float, int], Coroutine[Any, Any, Never]],
) -> None:
    await reducer("examples.dummy.reducer:FluorescenceReducer")
    await create_worker(
        Worker(
            name=WorkerName("w1"),
            settings=WorkerSettings(
                worker_class="examples.dummy.worker:FluorescenceWorker"
            ),
        )
    )
    await create_ingester(
        StreamingContrastIngester(
            name=StreamName("contrast"),
            settings=StreamingContrastSettings(upstream_url=Url("tcp://localhost:5556"),
                                               ingester_url=Url("tcp://localhost:10000")),
        )
    )
    await create_ingester(
        StreamingXspressIngester(
            name=StreamName("xspress3"),
            settings=StreamingXspressSettings(upstream_url=Url("tcp://localhost:9999"),
                                              ingester_url=Url("tcp://localhost:10001")),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"contrast", "xspress3"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        ntrig = 20
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "contrast": [[i] for i in range(ntrig)],
                "xspress3": [[i] for i in range(ntrig)],
            },
        )
        assert resp.status == 200
        uuid = await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_pkls(context, 9999, "tests/data/xspress3-dump.pkls",0.05, zmq.PUB))
    asyncio.create_task(stream_pkls(context, 5556, "tests/data/contrast-dump.pkls",0.05, zmq.PUB))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

        st = await session.get("http://localhost:5001/api/v1/result/pickle")
        content = await st.content.read()
        result = pickle.loads(content)
        print("content", result)
        assert len(result["results"]) == ntrig + 2

    context.destroy()

    print(content)
