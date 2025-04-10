import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish


@pytest.mark.asyncio
async def test_sequence(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
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
    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence",
            json={
                "parts": {
                    "main": {
                        "eiger": [
                            [
                                VirtualWorker(
                                    constraint=VirtualConstraint(2 * i)
                                ).model_dump(mode="json")
                            ]
                            for i in range(1, 5)
                        ],
                    },
                    "end": {
                        "eiger": [
                            [
                                VirtualWorker(
                                    constraint=VirtualConstraint(2 * i)
                                ).model_dump(mode="json")
                            ]
                            for i in range(1, 2)
                        ],
                    },
                },
                "sequence": ["main", "main", "end"],
            },
        )
        assert resp.status == 200

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

    await wait_for_finish()
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/mapping")
        content = await st.json()
        assert content == {
            "parts": {
                "main": {
                    "mapping": {
                        "eiger": [
                            [{"tags": ["generic"], "constraint": 2}],
                            [{"tags": ["generic"], "constraint": 4}],
                            [{"tags": ["generic"], "constraint": 6}],
                            [{"tags": ["generic"], "constraint": 8}],
                        ]
                    }
                },
                "end": {
                    "mapping": {"eiger": [[{"tags": ["generic"], "constraint": 2}]]}
                },
                "reserved_start_end": {
                    "mapping": {"eiger": [[{"tags": ["generic"], "constraint": None}]]}
                },
            },
            "sequence": [
                "reserved_start_end",
                "main",
                "main",
                "end",
                "reserved_start_end",
            ],
        }

    context.destroy()
