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
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
    WorkerTag,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish, monopart_sequence


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

    context = zmq.asyncio.Context()

    await wait_for_controller(streams={StreamName("eiger")}, workers={WorkerName("w1")})
    async with aiohttp.ClientSession() as session:
        await time_beacon(context, 9999, 5, flags=zmq.NOBLOCK)  # type: ignore[call-arg]
        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence",
            json=monopart_sequence(
                {
                    "eiger": [
                        [
                            VirtualWorker(
                                constraint=VirtualConstraint(2 * i),
                                tags={WorkerTag("debug")},
                            ).model_dump(mode="json")
                        ]
                        for i in range(1, ntrig)
                    ],
                }
            ),
        )
        assert resp.status == 200

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

    await wait_for_finish()

    context.destroy()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5002/api/v1/last_events?number=20")
        content = await st.content.read()
        res: list[EventData] = pickle.loads(content)
        got_frames: list[int] = []
        for ev in res:
            print("got event", ev.event_number, ev.streams.keys())
            pkt = parse(ev.streams[StreamName("eiger")])
            print(pkt)
            if isinstance(pkt, Stream1Data):
                got_frames.append(pkt.frame)
    assert got_frames == list(range(0, ntrig - 1))

    print(content)
