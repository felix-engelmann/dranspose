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
    EnsembleState,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker, WorkerSettings


@pytest.mark.parametrize(
    "filename, ntrig, nburst",
    [
        ("tests/data/maui-02-continuous-3.cbor", 3, 1),
        ("tests/data/maui-02-sequential-3.cbor", 3, 20),
    ],
)
@pytest.mark.asyncio
async def test_lecroy(
    filename: str,
    ntrig: int,
    nburst: int,
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pkls: Callable[
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

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"lecroy"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())
            logging.debug(f"Waiting for lecroy ingester {state.get_streams()=}")

        map = {
            "lecroy": [
                [VirtualWorker(constraint=VirtualConstraint(i)).model_dump(mode="json")]
                for i in range(1, ntrig)
            ],
        }
        logging.debug(f"Sending {map=}")
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json=map,
        )
        assert resp.status == 200
        await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(
        stream_pkls(
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
        i = 0
        while not content["finished"]:
            if i > 20:
                break
            i += 1
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
        logging.info(f"max_val {f['max_val']}")
        logging.info(f"ts {f['ts']}")
        assert f["max_val"].shape == (nburst,), "Unexpected shape of max_val result"
        assert f["ts"].shape == (nburst,), "Unexpected shape of ts result"

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)
