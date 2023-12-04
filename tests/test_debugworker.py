import asyncio
import logging
import os
import pickle
from typing import Awaitable, Callable, Any, Coroutine, Never, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.data.stream1 import Stream1Data, Stream1Packet
from dranspose.event import EventData
from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
from dranspose.middlewares import stream1
from dranspose.protocol import (
    EnsembleState,
    RedisKeys,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
    WorkerTag,
)

import redis.asyncio as redis

from dranspose.worker import Worker

from tests.fixtures import (
    controller,
    reducer,
    debug_worker,
    create_worker,
    create_ingester,
    stream_eiger,
    stream_orca,
    stream_alba,
)


@pytest.mark.asyncio
async def test_debugger(
    controller: None,
    debug_worker: Callable[[Optional[str], Optional[list[str]]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
) -> None:
    envbefore = os.environ
    await debug_worker("debugworker", ["debug"])
    assert "WORKER_TAGS" not in os.environ
    logging.warning("ENV %s", envbefore)
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5002/api/v1/last_event")
        content = await st.content.read()
        assert content == b""

        st = await session.get("http://localhost:5002/api/v1/status")
        assert True == await st.json()

    await create_worker(WorkerName("w1"))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while "w1" not in set([w.name for w in state.workers]):
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        assert set([w.name for w in state.workers]) == {"w1", "debugworker"}


@pytest.mark.asyncio
async def test_debug(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    debug_worker: Callable[[Optional[str], Optional[list[str]]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, Never]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await debug_worker("debugworker", ["debug"])
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

        assert set([w.name for w in state.workers]) == {"w1", "debugworker"}
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
                    if i % 4 != 0
                    else [
                        VirtualWorker(constraint=VirtualConstraint(2 * i)).model_dump(
                            mode="json"
                        ),
                        VirtualWorker(tags={WorkerTag("debug")}).model_dump(
                            mode="json"
                        ),
                    ]
                    for i in range(1, ntrig)
                ],
            },
        )
        assert resp.status == 200
        uuid = await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    context.destroy()

    await r.aclose()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5002/api/v1/last_event")
        content = await st.content.read()
        res: EventData = pickle.loads(content)
        pkg = stream1.parse(res.streams[StreamName("eiger")])
        print(pkg)
        if isinstance(pkg, Stream1Data):
            assert pkg.frame == 7
            assert pkg.shape == [1475, 831]
            assert pkg.type == "uint16"
        else:
            assert False
