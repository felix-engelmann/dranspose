import asyncio
import logging
import os
from pathlib import PosixPath
from typing import Awaitable, Callable, Coroutine, Optional, Any

import aiohttp
import h5pyd
import zmq.asyncio

import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqsub_lecroy import ZmqSubLecroyIngester, ZmqSubLecroySettings
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    ParameterName,
    VirtualConstraint,
)

from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller, set_uniform_sequence


@pytest.mark.parametrize(
    "filename, ntrig, nburst, max0, ts0",
    [
        (
            "tests/data/maui-02-continuous-3.cbor.gz",
            3,
            1,
            0.1675630532008654,
            1740563664.335,
        ),
        (
            "tests/data/maui-02-sequential-3.cbor.gz",
            3,
            20,
            0.022609806155742262,
            1740736919.435,
        ),
    ],
)
@pytest.mark.asyncio
async def test_lecroy(
    filename: str,
    ntrig: int,
    nburst: int,
    max0: float,
    ts0: float,
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_cbors: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
) -> None:
    await reducer("examples.parser.lecroy:LecroyReducer")
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.parser.lecroy:LecroyWorker",
            ),
        )
    )
    await create_ingester(
        ZmqSubLecroyIngester(
            settings=ZmqSubLecroySettings(
                ingester_streams=[StreamName("lecroy")],
                upstream_url=Url("tcp://localhost:22004"),
            ),
        )
    )

    await wait_for_controller(
        streams={StreamName("lecroy")}, parameters={ParameterName("channel")}
    )
    await set_uniform_sequence({StreamName("lecroy")}, ntrig)


    context = zmq.asyncio.Context()

    asyncio.create_task(
        stream_cbors(
            context,
            22004,
            PosixPath(filename),
            0.001,
            zmq.PUB,
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        logging.debug(f"Progress {content=}")
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()
            logging.debug(f"Progress {content=}")

        assert content == {
            "last_assigned": ntrig + 1,
            "completed_events": ntrig + 1,
            "total_events": ntrig + 1,
            "finished": True,
        }

    def work() -> None:
        f = h5pyd.File("http://localhost:5001/", "r")
        logging.info(
            f"file {f.keys()}",
        )
        assert f["max_val"][0] == max0
        assert f["ts"][0] == ts0
        assert f["max_val"].shape == (nburst,), "Unexpected shape of max_val result"
        assert f["ts"].shape == (nburst,), "Unexpected shape of ts result"

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)
