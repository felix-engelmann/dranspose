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
    RedisKeys,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
    WorkerTag,
)

import redis.asyncio as redis

from dranspose.worker import Worker


@pytest.mark.asyncio
async def test_outside(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    debug_worker: Callable[[Optional[str], Optional[list[str]]], Awaitable[None]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    time_beacon: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
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

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

    context = zmq.asyncio.Context()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"eiger"} - set(state.get_streams()) != set() or {"w1"} - set(
            state.get_workers()
        ) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())
        await time_beacon(context, 9999, 5, flags=zmq.NOBLOCK)
        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "eiger": [
                    [
                        VirtualWorker(
                            constraint=VirtualConstraint(2 * i),
                            tags={WorkerTag("debug")},
                        ).model_dump(mode="json")
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
    assert got_frames == list(range(0, ntrig - 1))

    print(content)
