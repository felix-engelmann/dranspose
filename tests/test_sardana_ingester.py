import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from fastapi import FastAPI
from pydantic import HttpUrl

from dranspose.ingester import Ingester
from dranspose.protocol import (
    EnsembleState,
    RedisKeys,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

import redis.asyncio as redis

from dranspose.worker import Worker

from dranspose.ingesters.http_sardana import app as custom_app


@pytest.mark.asyncio
async def test_sardana(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    http_ingester: Callable[[FastAPI, int, dict[str, Any]], Awaitable[None]],
    stream_sardana: Callable[[HttpUrl, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await http_ingester(custom_app, 5002, {"ingester_streams": ["sardana"]})

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"sardana"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "sardana": [
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

    updates = await r.xread({RedisKeys.updates(): 0})
    print("updates", updates)
    keys = await r.keys("dranspose:*")
    print("keys", keys)
    present_keys = {f"dranspose:assigned:{uuid}"}
    print("presentkeys", present_keys)
    assert present_keys - set(keys) == set()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_sardana(HttpUrl("http://localhost:5002"), ntrig - 1))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    context.destroy()

    await r.aclose()

    print(content)
    assert content["completed_events"] == ntrig + 1
    assert content["last_assigned"] == ntrig + 1
