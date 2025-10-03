import asyncio
import logging
from typing import Callable, Optional, Awaitable, Any, Coroutine

import h5pyd
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
)
from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.asyncio
async def test_empty_params(
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

    state = await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
        logging.warning("ensemble state uuids are %s", state.parameters_version)

        assert state.parameters_hash is not None
        assert state.reducer is not None
        assert state.reducer.parameters_hash is not None
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
        await set_uniform_sequence({StreamName("eiger")}, ntrig)

        context = zmq.asyncio.Context()

        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

        await wait_for_finish()

        def work() -> None:
            f = h5pyd.File("http://localhost:5001/", "r")
            logging.info(
                f"file {list(f.keys())}",
            )
            logging.warning("version %s", list(f["params"].keys()))

            logging.warning("str %s", f["params"]["bytes_param"])
            for t, v in [
                (str, b""),
                (bytes, b""),
                (int, 0),
                (float, 0.0),
                (bool, False),
            ]:
                logging.warning("param %s", f["params"][f"{t.__name__}_param"])
                assert f["params"][f"{t.__name__}_param"][()] == v

            for t, v in [
                (str, b""),
                (bytes, b""),
                (int, 0),
                (float, 0.0),
                (bool, False),
            ]:
                logging.info("work param %s", f["worker_params"][f"{t.__name__}_param"])
                assert f["worker_params"][f"{t.__name__}_param"][()] == v

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, work)

        context.destroy()

    server.should_exit = True
    await server_task
    await asyncio.sleep(0.1)
