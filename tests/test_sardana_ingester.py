import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from fastapi import FastAPI
from pydantic import HttpUrl

from dranspose.ingester import Ingester
from dranspose.protocol import (
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker

from dranspose.ingesters.http_sardana import app as custom_app
from tests.utils import wait_for_controller, wait_for_finish


@pytest.mark.asyncio
async def test_sardana(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    http_ingester: Callable[[FastAPI, int, dict[str, Any]], Awaitable[None]],
    stream_sardana: Callable[[HttpUrl, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await http_ingester(custom_app, 5002, {"ingester_streams": ["sardana"]})

    await wait_for_controller(streams={"sardana"})
    async with aiohttp.ClientSession() as session:
        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "sardana": [
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

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_sardana(HttpUrl("http://localhost:5002"), ntrig - 1))

    content = await wait_for_finish()

    context.destroy()

    assert content == {
        "last_assigned": ntrig + 1,
        "completed_events": ntrig + 1,
        "total_events": ntrig + 1,
        "finished": True,
    }
