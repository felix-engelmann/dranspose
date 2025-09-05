import logging

import asyncio
from typing import Awaitable, Callable, Optional, Coroutine, Any
import zmq.asyncio

import aiohttp
import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.ingesters.tcp_positioncap import (
    TcpPcapIngester,
    TcpPcapSettings,
)

from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish, monopart_sequence, vworker


@pytest.mark.asyncio
async def test_multiple_scans(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pcap: Callable[[int, int, int], Coroutine[None, None, None]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )
    await create_ingester(
        TcpPcapIngester(
            settings=TcpPcapSettings(
                ingester_streams=[StreamName("pcap")],
                upstream_url=Url("tcp://localhost:8889"),
                ingester_url=Url("tcp://localhost:10011"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("eiger"), StreamName("pcap")})
    async with aiohttp.ClientSession() as session:
        ntrig = 20
        mp = {
            "eiger": [[vworker(i)] if i % 12 < 10 else None for i in range(ntrig)],
            "pcap": [[vworker(i)] for i in range(ntrig)],
        }
        logging.info("map is %s", mp)
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence",
            json=monopart_sequence(mp),
        )
        assert resp.status == 200
        await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

    asyncio.create_task(stream_pcap(9, 8889, 2))

    await wait_for_finish()

    context.destroy()
