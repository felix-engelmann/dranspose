import asyncio
import pickle
from typing import Awaitable, Callable, Any, Coroutine, Never, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.event import InternalWorkerMessage
from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
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
async def test_dump(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, Never]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, Never]],
    stream_alba: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, Never]],
    tmp_path: Any,
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_worker(WorkerName("w2"))
    await create_worker(WorkerName("w3"))

    p_eiger = tmp_path / "eiger_dump.pkls"
    print(p_eiger, type(p_eiger))

    await create_ingester(
        StreamingSingleIngester(
            name=StreamName("eiger"),
            settings=StreamingSingleSettings(
                upstream_url=Url("tcp://localhost:9999"), dump_path=p_eiger
            ),
        )
    )
    await create_ingester(
        StreamingSingleIngester(
            name=StreamName("orca"),
            settings=StreamingSingleSettings(
                upstream_url=Url("tcp://localhost:9998"),
                ingester_url=Url("tcp://localhost:10011"),
            ),
        )
    )
    await create_ingester(
        StreamingSingleIngester(
            name=StreamName("alba"),
            settings=StreamingSingleSettings(
                upstream_url=Url("tcp://localhost:9997"),
                ingester_url=Url("tcp://localhost:10012"),
            ),
        )
    )
    await create_ingester(
        StreamingSingleIngester(
            name=StreamName("slow"),
            settings=StreamingSingleSettings(
                upstream_url=Url("tcp://localhost:9996"),
                ingester_url=Url("tcp://localhost:10013"),
            ),
        )
    )

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        print("content", state.ingesters)
        while {"eiger", "orca", "alba", "slow"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        print("startup done")
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
                    for i in range(1, ntrig)
                ],
                "orca": [
                    [
                        VirtualWorker(
                            constraint=VirtualConstraint(2 * i + 1)
                        ).model_dump(mode="json")
                    ]
                    for i in range(1, ntrig)
                ],
                "alba": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(2 * i)).model_dump(
                            mode="json"
                        ),
                        VirtualWorker(
                            constraint=VirtualConstraint(2 * i + 1)
                        ).model_dump(mode="json"),
                    ]
                    for i in range(1, ntrig)
                ],
                "slow": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(2 * i)).model_dump(
                            mode="json"
                        ),
                        VirtualWorker(
                            constraint=VirtualConstraint(2 * i + 1)
                        ).model_dump(mode="json"),
                    ]
                    if i % 4 == 0
                    else None
                    for i in range(1, ntrig)
                ],
            },
        )
        assert resp.status == 200
        uuid = await resp.json()

    print("uuid", uuid, type(uuid))
    updates = await r.xread({RedisKeys.updates(): 0})
    print("updates", updates)
    keys = await r.keys("dranspose:*")
    print("keys", keys)
    present_keys = {f"dranspose:ready:{uuid}"}
    print("presentkeys", present_keys)
    assert present_keys - set(keys) == set()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))
    asyncio.create_task(stream_orca(context, 9998, ntrig - 1))
    asyncio.create_task(stream_alba(context, 9997, ntrig - 1))
    asyncio.create_task(stream_alba(context, 9996, ntrig // 4))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    # read dump
    with open(p_eiger, "rb") as f:
        evs = []
        while True:
            try:
                dat: InternalWorkerMessage = pickle.load(f)
                evs.append(dat.event_number)
                print("loaded dump type", type(dat))
            except EOFError:
                break

    print(evs)
    assert evs == list(range(0, ntrig + 1))

    context.destroy()

    await r.aclose()

    print(content)
