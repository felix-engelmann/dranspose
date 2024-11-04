import asyncio
import logging
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from fastapi import FastAPI
from pydantic import HttpUrl

from dranspose.ingester import Ingester
from dranspose.protocol import (
    EnsembleState,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker

from dranspose.ingesters.http_sardana import app as custom_app


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

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"sardana"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            logging.warning("config not ready %s", state)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

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

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    context.destroy()

    assert content == {
        "last_assigned": ntrig + 1,
        "completed_events": ntrig + 1,
        "total_events": ntrig + 1,
        "finished": True,
    }
