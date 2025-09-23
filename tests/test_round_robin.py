import asyncio
import logging
from typing import Awaitable, Callable, Any, Coroutine, Optional

from dranspose.protocol import (
    StreamName,
    WorkerName,
)
from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_finish, wait_for_controller, vworker, monopart_sequence


async def consume_round_robin(ctx: zmq.Context[Any], ports: list[int], num: int) -> int:
    sockets = []
    for p in ports:
        s = ctx.socket(zmq.PULL)
        s.connect(f"tcp://127.0.0.1:{p}")
        sockets.append(s)

    rcvd = 0
    si = 0
    while rcvd < num:
        data = await sockets[si].recv_multipart(copy=False)
        si += 1
        si = si % len(ports)
        rcvd += 1
        logging.info("received %d th data %s", rcvd, data)

    return rcvd


@pytest.mark.asyncio
async def test_roundrobin(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName | Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[
        [zmq.Context[Any], int, int, float], Coroutine[Any, Any, None]
    ],
) -> None:
    await reducer(None)
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w5556"),
                worker_class="examples.repub.worker:RepubWorker",
            ),
        )
    )
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w5557"),
                worker_class="examples.repub.worker:RepubWorker",
            ),
        )
    )
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w5558"),
                worker_class="examples.repub.worker:RepubWorker",
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

    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
        ntrig = 10
        sequence = monopart_sequence(
            {"eiger": [[vworker(i % 3)] for i in range(1, ntrig)]}
        )
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence", json=sequence
        )
        assert resp.status == 200
        await resp.json()

    with zmq.asyncio.Context() as context:
        collector = asyncio.create_task(
            consume_round_robin(context, [5556, 5557, 5558], ntrig - 1 + 2 * 3)
        )
        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1, 0.1))
        await wait_for_finish()
        await collector
