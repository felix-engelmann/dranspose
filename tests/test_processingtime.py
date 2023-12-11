import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional

from pydantic import TypeAdapter

from dranspose.protocol import (
    EnsembleState,
    StreamName,
    WorkerName,
    WorkerTimes,
    VirtualWorker,
    VirtualConstraint,
    EventNumber,
    SystemLoadType,
)
from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.worker import Worker, WorkerSettings


@pytest.mark.asyncio
async def test_simple(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName | Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[
        [zmq.Context[Any], int, int, float], Coroutine[Any, Any, None]
    ],
) -> None:
    async with aiohttp.ClientSession() as session:
        st = await session.get(
            "http://localhost:5000/api/v1/load?intervals=1&intervals=10&scan=True"
        )
        load = await st.json()
        assert load == {}
    await reducer(None)
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.slow.worker:SlowWorker",
            ),
        )
    )
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w2"),
                worker_class="examples.slow.worker:SlowWorker",
            ),
        )
    )
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w3"),
                worker_class="examples.slow.worker:SlowWorker",
            ),
        )
    )
    await create_ingester(
        StreamingSingleIngester(
            name=StreamName("eiger"),
            settings=StreamingSingleSettings(upstream_url=Url("tcp://localhost:9999")),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"eiger"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        st = await session.get(
            "http://localhost:5000/api/v1/load?intervals=1&intervals=10&scan=True"
        )
        load = await st.json()
        assert load == {}

        ntrig = 100
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
            },
        )
        assert resp.status == 200
        await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1, 0.1))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    context.destroy()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/status")
        content = await st.json()
        wtimes = TypeAdapter(dict[WorkerName, dict[EventNumber, WorkerTimes]])
        times = wtimes.validate_python(content["processing_times"])
        print(times)
        st = await session.get(
            "http://localhost:5000/api/v1/load?intervals=1&intervals=10&scan=True"
        )
        load = await st.json()
        lta = TypeAdapter(SystemLoadType)
        system_load = lta.validate_python(load)
        print("")
        print(system_load)
        for wn, wl in system_load.items():
            assert wl.last_event == 100
            assert wl.intervals["scan"].load > 0.8
        assert sum([len(e) for e in times.values()]) == ntrig - 1 + 2 * 3
