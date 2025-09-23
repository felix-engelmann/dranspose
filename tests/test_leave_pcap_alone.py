import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from typing import Awaitable, Callable, Coroutine, Optional, Any


import pytest
import zmq
from pydantic_core import Url

from dranspose.helpers.utils import cancel_and_wait
from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.ingesters.tcp_positioncap import TcpPcapIngester, TcpPcapSettings
from dranspose.protocol import (
    StreamName,
    WorkerName,
)

from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    logging.error("ingester connected to pcap")


@pytest.mark.asyncio
async def test_pcapingester(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    server = await asyncio.start_server(handle_client, "localhost", 8889)
    task = asyncio.create_task(server.serve_forever())

    await reducer(None)
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.parser.positioncap:PcapWorker",
            ),
        )
    )
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
                ingester_url=Url("tcp://localhost:10012"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("pcap"), StreamName("eiger")})
    ntrig = 10
    await set_uniform_sequence({StreamName("eiger")}, ntrig)
    with zmq.asyncio.Context() as context:
        await stream_eiger(context, 9999, ntrig - 1)
        content = await wait_for_finish()
        await cancel_and_wait(task)
    print(content)
