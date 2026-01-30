import asyncio
import logging
import threading

from typing import Callable, Optional, Awaitable, Any, Coroutine

import h5pyd
import aiohttp
import pytest
import zmq
from pydantic_core import Url

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
from dranspose.replay import replay
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.asyncio
async def test_timer_params(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
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
            assert f["params"]["roi1"][()] == b"bla"
            assert f["worker_params"]["roi1"][()] == b"bla"

        def work2() -> None:
            f = h5pyd.File("http://localhost:5001/", "r")
            logging.info(
                f"file {list(f.keys())}",
            )
            logging.warning("version %s", list(f["params"].keys()))
            assert f["params"]["roi1"][()] == b"new_value"
            assert f["worker_params"]["roi1"][()] == b"bla"

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, work)

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/roi1",
            data=b"new_value",
        )
        assert resp.status == 200

        await asyncio.sleep(1.5)

        await loop.run_in_executor(None, work2)

        context.destroy()


@pytest.mark.asyncio
async def test_timer_replay(
    tmp_path: Any,
) -> None:
    stop_event = threading.Event()
    done_event = threading.Event()

    thread = threading.Thread(
        target=replay,
        args=(
            "examples.params.worker:ParamWorker",
            "examples.params.reducer:ParamReducer",
            None,
            "examples.dummy.source:FluorescenceSource",
            None,
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    thread.start()

    done_event.wait()

    def work() -> None:
        f = h5pyd.File("http://localhost:5010/", "r")
        logging.info(
            f"file {list(f.keys())}",
        )
        logging.warning("version %s", list(f["params"].keys()))
        assert f["params"]["roi1"][()] == b"bla"
        assert f["worker_params"]["roi1"][()] == b"bla"

    def work2() -> None:
        f = h5pyd.File("http://localhost:5010/", "r")
        logging.info(
            f"file {list(f.keys())}",
        )
        logging.warning("version %s", list(f["params"].keys()))
        assert f["params"]["roi1"][()] == b"new_value"
        assert f["worker_params"]["roi1"][()] == b"bla"

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5010/api/v1/parameter/roi1",
            data=b"new_value",
        )
        assert resp.status == 200

    await asyncio.sleep(1.5)

    await loop.run_in_executor(None, work2)

    stop_event.set()

    thread.join()
