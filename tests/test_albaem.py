import asyncio
import os
from pathlib import PosixPath
from typing import Awaitable, Callable, Coroutine, Optional, Any

import zmq.asyncio

import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqsub_albaem import ZmqSubAlbaemIngester, ZmqSubAlbaemSettings
from dranspose.protocol import (
    StreamName,
    WorkerName,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.asyncio
async def test_albaem(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_cbors: Callable[
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
    await set_uniform_sequence(streams={StreamName("albaem")}, ntrig=5)

    context = zmq.asyncio.Context()

    asyncio.create_task(
        stream_cbors(
            context,
            22004,
            PosixPath("tests/data/albaem-dump.cbors"),
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
