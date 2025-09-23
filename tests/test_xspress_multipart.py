import os
import pathlib
from pathlib import PosixPath

import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional
import zmq.asyncio

import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqsub_xspress3 import (
    ZmqSubXspressIngester,
    ZmqSubXspressSettings,
)
from dranspose.protocol import (
    StreamName,
    WorkerName,
)
from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.asyncio
async def test_multipart(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_cbors: Callable[
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
        ZmqSubXspressIngester(
            settings=ZmqSubXspressSettings(
                ingester_streams=[StreamName("xspress3")],
                upstream_url=Url("tcp://localhost:9999"),
                ingester_url=Url("tcp://localhost:10001"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("xspress3")})
    ntrig = 5
    await set_uniform_sequence({StreamName("xspress3")}, ntrig)

    with zmq.asyncio.Context() as context:
        asyncio.create_task(
            stream_cbors(
                context,
                9999,
                PosixPath("tests/data/xspress3-multipart.cbors"),
                0.001,
                zmq.PUB,
            )
        )
        content = await wait_for_finish()
    print(content)
