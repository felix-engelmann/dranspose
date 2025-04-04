import asyncio
import os
from pathlib import PosixPath
from typing import Awaitable, Callable, Coroutine, Optional, Any

import aiohttp
import zmq.asyncio

import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_eiger_legacy import (
    ZmqPullEigerLegacyIngester,
    ZmqPullEigerLegacySettings,
)
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller, wait_for_finish


@pytest.mark.asyncio
async def test_eiger_legacy(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pkls: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
) -> None:
    await reducer(None)
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.parser.eigerlegacy:LegacyWorker",
            ),
        )
    )
    await create_ingester(
        ZmqPullEigerLegacyIngester(
            settings=ZmqPullEigerLegacySettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:22005"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
        ntrig = 3
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "eiger": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
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
            22005,
            PosixPath("tests/data/eiger-small.pkls"),
            0.001,
            zmq.PUSH,
            begin=0,  # type: ignore[call-arg]
        )
    )

    content = await wait_for_finish()

    print(content)
