import asyncio
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
from dranspose.middlewares.stream1 import parse
from dranspose.protocol import (
    EnsembleState,
    StreamName,
    WorkerName,
    VirtualWorker,
    WorkerTag,
)

from dranspose.worker import Worker


@pytest.mark.asyncio
async def test_simple(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    debug_worker: Callable[[Optional[str], Optional[list[str]]], Awaitable[None]],
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
                    []
                    if 3 < i < 8
                    else [
                        VirtualWorker(tags={WorkerTag("debug")}).model_dump(mode="json")
                    ]
                    for i in range(1, ntrig)
                ],
            },
        )
        assert resp.status == 200

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

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5002/api/v1/last_events?number=20")
        content = await st.content.read()
        res: list[EventData] = pickle.loads(content)
        got_frames = []
        for ev in res:
            print("got event", ev.event_number, ev.streams.keys())
            pkt = parse(ev.streams[StreamName("eiger")])
            print(pkt)
            if isinstance(pkt, Stream1Data):
                got_frames.append(pkt.frame)
    assert got_frames == list(range(0, 3)) + list(range(7, 9))
