import asyncio
import logging
import pickle
from typing import Callable, Optional, Awaitable, Any, Coroutine

import aiohttp
import pytest
import uvicorn
import zmq
from pydantic_core import Url

from dranspose.controller import app
from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.parameters import ParameterList
from dranspose.protocol import (
    WorkerName,
    StreamName,
    EnsembleState,
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.worker import Worker, WorkerSettings


@pytest.mark.asyncio
async def test_empty_params(
    # controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    config = uvicorn.Config(app, port=5000, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)

    logging.info("started controller")
    await asyncio.sleep(1)

    await reducer("examples.params.reducer:ParamReducer")
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.params.worker:ParamWorker",
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

    await asyncio.sleep(1)

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"eiger"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        logging.warning("ensemble state uuids are %s", state.parameters_version)

        assert state.parameters_hash == state.reducer.parameters_hash
        for ing in state.ingesters:
            assert state.parameters_hash == ing.parameters_hash
        for wo in state.workers:
            assert state.parameters_hash == wo.parameters_hash

        par = await session.get("http://localhost:5000/api/v1/parameters")
        assert par.status == 200
        params = ParameterList.validate_python(await par.json())

        logging.warning("params %s", params)

        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "eiger": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(1, ntrig)
                ],
            },
        )
        assert resp.status == 200

        context = zmq.asyncio.Context()

        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

        async with aiohttp.ClientSession() as session:
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()
            while not content["finished"]:
                await asyncio.sleep(0.3)
                st = await session.get("http://localhost:5000/api/v1/progress")
                content = await st.json()

            st = await session.get("http://localhost:5001/api/v1/result/pickle")
            content = await st.content.read()
            result = pickle.loads(content)
            logging.warning("content in reducer is %s", result["params"])
            logging.warning("content in worker is %s", result["worker_params"])
            params = result["params"]
            for t, v in [
                (str, ""),
                (bytes, b""),
                (int, 0),
                (float, 0.0),
                (bool, False),
            ]:
                logging.info("param %s", params[f"{t.__name__}_param"])
                assert params[f"{t.__name__}_param"].value == v
            params = result["worker_params"]
            for t, v in [
                (str, ""),
                (bytes, b""),
                (int, 0),
                (float, 0.0),
                (bool, False),
            ]:
                logging.info("work param %s", params[f"{t.__name__}_param"])
                assert params[f"{t.__name__}_param"].value == v

        context.destroy()

    server.should_exit = True
    await server_task
    await asyncio.sleep(0.1)
