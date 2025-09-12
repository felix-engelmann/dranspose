import asyncio
import logging
from typing import Awaitable, Callable, Any, Coroutine, Optional
import h5pyd

import aiohttp
import numpy as np

import pytest
from _pytest.fixtures import FixtureRequest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.stins_parallel import (
    StinsParallelIngester,
    StinsParallelSettings,
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
async def test_parallel(
    request: FixtureRequest,
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer("tests.aux_payloads:TestReducer")
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="tests.aux_payloads:TestWorker",
            ),
        )
    )
    ing1 = await create_ingester(
        StinsParallelIngester(
            settings=StinsParallelSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )
    ing2 = await create_ingester(
        StinsParallelIngester(
            settings=StinsParallelSettings(
                ingester_name="eiger-2",
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
                ingester_url=Url("tcp://localhost:10011"),
            ),
        )
    )

    async with aiohttp.ClientSession() as session:
        await wait_for_controller(
            streams={StreamName("eiger")}, workers={WorkerName("w1")}
        )

        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "eiger": [
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

    with zmq.asyncio.Context() as context:
        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

        content = await wait_for_finish()

        assert content == {
            "last_assigned": ntrig + 1,
            "completed_events": ntrig + 1,
            "total_events": ntrig + 1,
            "finished": True,
        }

        logging.info("proc1 ev %d", ing1.state.processed_events)
        logging.info("proc2 ev %d", ing2.state.processed_events)
        assert ing1.state.processed_events < ntrig
        assert ing2.state.processed_events < ntrig
        assert ing1.state.processed_events + ing2.state.processed_events == ntrig + 1

    def work() -> None:
        f = h5pyd.File("http://localhost:5001/", "r")
        assert f[f"results/{0}/eiger/htype"][()] == b"header"
        assert f[f"results/{ntrig}/eiger/htype"][()] == b"series_end"
        for i in range(1, ntrig):
            assert f[f"results/{i}/eiger/msg_number"][()] == i
            assert f[f"results/{i}/eiger/htype"][()] == b"image"
            assert f[f"results/{i}/eiger/frame"][()] == i - 1
            assert np.array_equal(f[f"results/{i}/eiger/shape"][:], [1475, 831])

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)
