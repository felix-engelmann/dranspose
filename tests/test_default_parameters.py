import asyncio
import logging
from typing import Callable, Optional, Awaitable

import aiohttp
import pytest
from pydantic_core import Url


from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.parameters import ParameterList
from dranspose.protocol import WorkerName, StreamName, EnsembleState
from dranspose.worker import Worker, WorkerSettings


@pytest.mark.asyncio
async def test_default_params(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
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
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"eiger"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        par = await session.get("http://localhost:5000/api/v1/parameters")
        assert par.status == 200
        params = ParameterList.validate_python(await par.json())

        logging.warning("params %s", params)

        resp = await session.get(
            "http://localhost:5000/api/v1/parameter/roi1",
        )
        assert resp.status == 200
        assert await resp.content.read() == b"bla"

        resp = await session.get(
            "http://localhost:5000/api/v1/parameter/file_parameter",
        )
        assert resp.status == 200
        assert await resp.content.read() == b""
