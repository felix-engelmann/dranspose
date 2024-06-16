import asyncio
import logging
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.protocol import (
    EnsembleState,
    RedisKeys,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

import redis.asyncio as redis

from dranspose.worker import Worker


@pytest.mark.asyncio
async def test_simple(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_small: Callable[
        [zmq.Context[Any], int, int, Optional[float]], Coroutine[Any, Any, None]
    ],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w2"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w3"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w4"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w5"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w6"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w7"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w8"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w9"), subprocess=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w10"), subprocess=True)  # type: ignore[call-arg]
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        ),
        subprocess=True,  # type: ignore[call-arg]
    )

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"eiger"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        ntrig = 100
        mapping = {
            "eiger": [
                [
                    VirtualWorker(constraint=VirtualConstraint(i // 10)).model_dump(
                        mode="json"
                    )
                ]
                for i in range(1, ntrig)
            ],
        }
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json=mapping,
        )
        assert resp.status == 200
        uuid = await resp.json()

    updates = await r.xread({RedisKeys.updates(): 0})
    print("updates", updates)
    keys = await r.keys("dranspose:*")
    print("keys", keys)
    present_keys = {f"dranspose:assigned:{uuid}"}
    print("presentkeys", present_keys)

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_small(context, 9999, ntrig - 1, 0.000001))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.9)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()
            st = await session.get("http://localhost:5000/api/v1/config")
            conf = EnsembleState.model_validate(await st.json())
            msg = []
            for k in conf.workers + conf.ingesters + [conf.reducer]:
                if k is not None:
                    msg.append(f"{k.name}:{k.processed_events} -- {k.event_rate}")
            logging.info("config is \n%s", "\n".join(msg))

    context.destroy()

    await r.aclose()

    print(content)
