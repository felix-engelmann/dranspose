import asyncio
import os
import pickle
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.data.stream1 import Stream1Data
from dranspose.event import EventData
from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.middlewares import stream1
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
    WorkerTag,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish


@pytest.mark.asyncio
async def test_debugger(
    controller: None,
    debug_worker: Callable[[Optional[str], Optional[list[str]]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
) -> None:
    await debug_worker("debugworker", ["debug"])
    assert "WORKER_TAGS" not in os.environ
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5002/api/v1/last_events?number=1")
        content = await st.content.read()
        res: list[EventData] = pickle.loads(content)
        assert res == []

        st = await session.get("http://localhost:5002/api/v1/status")
        assert await st.json()

    await create_worker(WorkerName("w1"))

    state = await wait_for_controller(workers={WorkerName("w1")})

    assert set([w.name for w in state.workers]) == {"w1", "debugworker"}


@pytest.mark.asyncio
async def test_debug(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    debug_worker: Callable[[Optional[str], Optional[list[str]]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await debug_worker("debugworker", ["debug"])
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )

    state = await wait_for_controller(streams={StreamName("eiger")})
    assert set([w.name for w in state.workers]) == {"w1", "debugworker"}
    async with aiohttp.ClientSession() as session:
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
        await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

    await wait_for_finish()

    context.destroy()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5002/api/v1/last_events?number=2")
        content = await st.content.read()
        res: list[EventData] = pickle.loads(content)
        for ev in res:
            print("got event", ev.event_number, ev.streams.keys())
        pkg = stream1.parse(res[0].streams[StreamName("eiger")])
        print(pkg)
        if isinstance(pkg, Stream1Data):
            assert pkg.frame == 7
            assert pkg.shape == [1475, 831]
            assert pkg.type == "uint16"
        else:
            assert False
