import asyncio
import logging
from typing import Callable, Any, Coroutine, Optional, Awaitable

import aiohttp
import pytest
import zmq.asyncio

from dranspose.protocol import (
    EnsembleState,
    VirtualWorker,
    VirtualConstraint,
    WorkerName,
)
from dranspose.worker import Worker

rust = pytest.mark.skipif("not config.getoption('rust')")


@rust
@pytest.mark.asyncio
async def test_rust_basic(
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    stream_small: Callable[
        [zmq.Context[Any], int, int, Optional[float]], Coroutine[Any, Any, None]
    ],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"), subprocess=True)  # type: ignore[call-arg]

    await asyncio.sleep(0.5)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        print("content", state.ingesters)
        while {"eiger"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        logging.info("startup done")

        ntrig = 15
        mapping = {
            "eiger": [
                [VirtualWorker(constraint=VirtualConstraint(i)).model_dump(mode="json")]
                for i in range(1, ntrig)
            ],
        }
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json=mapping,
        )
        assert resp.status == 200

    context = zmq.asyncio.Context()
    await stream_small(context, 9999, 15, 0.000001)

    await asyncio.sleep(10)

    context.destroy()
