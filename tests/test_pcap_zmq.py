import asyncio
import os
from pathlib import PosixPath
from typing import Awaitable, Callable, Coroutine, Optional, Any

import aiohttp
import zmq.asyncio

import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqsub_pcap import ZmqSubPCAPIngester, ZmqSubPCAPSettings
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_finish, wait_for_controller, set_uniform_sequence


@pytest.mark.asyncio
async def test_pcap(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_cbors: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
) -> None:
    await reducer(None)
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.parser.pcap:PcapWorker",
            ),
        )
    )
    await create_ingester(
        ZmqSubPCAPIngester(
            settings=ZmqSubPCAPSettings(
                ingester_streams=[StreamName("pcap")],
                upstream_url=Url("tcp://localhost:22004"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("pcap")})
    ntrig = 1500
    await set_uniform_sequence({StreamName("pcap")}, ntrig)

    with zmq.asyncio.Context() as context:
        asyncio.create_task(
            stream_cbors(
                context,
                22004,
                PosixPath("tests/data/pcap-zmq.cbors"),
                0.001,
                zmq.PUB,
            )
        )
        content = await wait_for_finish()
    print(content)
