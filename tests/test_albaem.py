import asyncio
import os
from pathlib import PosixPath
from typing import Awaitable, Callable, Coroutine, Optional, Any

import aiohttp
import zmq.asyncio

import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqsub_albaem import ZmqSubAlbaemIngester, ZmqSubAlbaemSettings
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish


@pytest.mark.asyncio
async def test_albaem(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pkls: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_ingester(
        ZmqSubAlbaemIngester(
            settings=ZmqSubAlbaemSettings(
                ingester_streams=[StreamName("albaem")],
                upstream_url=Url("tcp://localhost:22004"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("albaem")})
    async with aiohttp.ClientSession() as session:
        ntrig = 5
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "albaem": [
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

    asyncio.create_task(
        stream_pkls(
            context,
            22004,
            PosixPath("tests/data/albaem-dump.pkls"),
            0.001,
            zmq.PUB,
        )
    )

    content = await wait_for_finish()

    assert content == {
        "last_assigned": 6,
        "completed_events": 6,
        "total_events": 6,
        "finished": True,
    }
