import os
import pathlib
from pathlib import PosixPath

import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional
import zmq.asyncio

import aiohttp
import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.tcp_positioncap import TcpPcapSettings
from dranspose.ingesters.zmqsub_contrast import (
    ZmqSubContrastIngester,
    ZmqSubContrastSettings,
)
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.worker import Worker, WorkerSettings
from examples.dummy.sw_trig_ingester import SoftTriggerPcapIngester
from tests.utils import wait_for_controller, wait_for_finish


@pytest.mark.asyncio
async def test_ingester(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pcap: Callable[[int], Coroutine[None, None, None]],
    stream_pkls: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
    tmp_path: pathlib.PurePath,
) -> None:
    await reducer("examples.dummy.reducer:FluorescenceReducer")
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.dummy.worker:FluorescenceWorker",
            ),
        )
    )

    await create_ingester(
        ZmqSubContrastIngester(
            settings=ZmqSubContrastSettings(
                ingester_streams=[StreamName("contrast")],
                upstream_url=Url("tcp://localhost:5556"),
                ingester_url=Url("tcp://localhost:10000"),
            ),
        )
    )

    ntrig = 20

    await create_ingester(
        SoftTriggerPcapIngester(
            settings=TcpPcapSettings(
                ingester_streams=[StreamName("pcap"), StreamName("dummy")],
                upstream_url=Url("tcp://localhost:8889"),
                ingester_url=Url("tcp://localhost:10001"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("contrast"), StreamName("pcap")})
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "contrast": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(ntrig)
                ],
                "pcap": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(ntrig)
                ],
                "dummy": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(ntrig)
                ],
            },
        )
        assert resp.status == 200
        await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_pcap(ntrig))

    asyncio.create_task(
        stream_pkls(
            context, 5556, PosixPath("tests/data/contrast-dump.pkls"), 0.001, zmq.PUB
        )
    )

    content = await wait_for_finish()

    context.destroy()

    print(content)
