import asyncio
import os
from pathlib import PosixPath
from typing import Awaitable, Callable, Coroutine, Optional, Any

import aiohttp
import zmq.asyncio

import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqsub_pcap import ZmqSubPCAPIngester, ZmqSubPCAPSettings
from dranspose.protocol import (
    EnsembleState,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker


@pytest.mark.asyncio
async def test_albaem(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pkls: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_ingester(
        ZmqSubPCAPIngester(
            settings=ZmqSubPCAPSettings(
                ingester_streams=[StreamName("pcap")],
                upstream_url=Url("tcp://localhost:22004"),
            ),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"pcap"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        ntrig = 5
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "pcap": [
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
        await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(
        stream_pkls(
            context,
            22004,
            PosixPath("tests/data/pcap-zmq.pkls"),
            0.001,
            zmq.PUB,
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    print(content)
