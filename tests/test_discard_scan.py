import asyncio
import pickle
from datetime import datetime, timezone, timedelta
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp
import numpy as np

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
    WorkerTag,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish


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

    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
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

    await wait_for_finish()

    context.destroy()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5002/api/v1/last_events?number=20")
        content = await st.content.read()
        res: list[EventData] = pickle.loads(content)
        got_frames: list[int] = []
        for ev in res:
            assert set(ev.streams.keys()) == {"eiger"}
            pkt = parse(ev.streams[StreamName("eiger")])
            if isinstance(pkt, Stream1Data):
                assert pkt.frame == ev.event_number - 1
                assert pkt.shape == [1475, 831]
                assert pkt.type == "uint16"
                assert pkt.compression == "none"
                assert "dummy" in pkt.timestamps
                assert (
                    datetime.now(timezone.utc) - timedelta(seconds=60)
                    < datetime.fromisoformat(pkt.timestamps["dummy"])
                    < datetime.now(timezone.utc)
                )
                assert pkt.data.dtype == np.uint16
                got_frames.append(pkt.frame)
    assert got_frames == list(range(0, 3)) + list(range(7, 9))
