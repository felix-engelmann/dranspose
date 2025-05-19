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
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
    EnsembleState,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller


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
    await create_worker(WorkerName("w1"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w2"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w3"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w4"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w5"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w6"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w7"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w8"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w9"), threaded=True)  # type: ignore[call-arg]
    await create_worker(WorkerName("w10"), threaded=True)  # type: ignore[call-arg]
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        ),
        threaded=True,  # type: ignore[call-arg]
    )

    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
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

        assert content == {
            "last_assigned": ntrig + 1,
            "completed_events": ntrig + 1,
            "total_events": ntrig + 1,
            "finished": True,
        }
    context.destroy()
