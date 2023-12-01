import asyncio
import logging
from typing import Callable, Optional, Awaitable, Any, Coroutine, Never

import aiohttp
import pytest
import zmq
from pydantic import UUID4
from pydantic_core import Url
import redis.asyncio as redis


from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
from dranspose.protocol import WorkerName, StreamName, EnsembleState
from dranspose.worker import Worker

from tests.fixtures import (
    controller,
    reducer,
    create_worker,
    create_ingester,
)


@pytest.mark.asyncio
async def test_params(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_ingester(
        StreamingSingleIngester(
            name=StreamName("eiger"),
            settings=StreamingSingleSettings(upstream_url=Url("tcp://localhost:9999")),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"eiger"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        resp = await session.post(
            "http://localhost:5000/api/v1/parameters/json",
            json={"roi1": [0, 10]},
        )
        assert resp.status == 200
        uuid = await resp.json()

        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while str(state.workers[0].parameters_uuid) != uuid:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())
            logging.warning("got state %s", state)
