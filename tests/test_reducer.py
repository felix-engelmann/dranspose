import pickle

import redis.asyncio as redis
import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Never, Optional
import zmq.asyncio

import aiohttp
import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
from dranspose.protocol import EnsembleState, RedisKeys, StreamName, WorkerName
from dranspose.worker import Worker, WorkerSettings

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
async def test_reduction(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, Never]],
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
        StreamingSingleIngester(
            name=StreamName("eiger"),
            settings=StreamingSingleSettings(upstream_url=Url("tcp://localhost:9999")),
        )
    )

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

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
                "eiger": [[2 * i] for i in range(1, ntrig)],
            },
        )
        assert resp.status == 200
        uuid = await resp.json()

    updates = await r.xread({RedisKeys.updates(): 0})
    print("updates", updates)
    keys = await r.keys("dranspose:*")
    print("keys", keys)
    present_keys = {f"dranspose:assigned:{uuid}"}
    print("presentkeys", present_keys)
    assert present_keys - set(keys) == set()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

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
        assert len(result["results"]) == ntrig + 1

    context.destroy()

    await r.aclose()

    print(content)
