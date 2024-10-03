import os
import pathlib
from pathlib import PosixPath

import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional
import zmq.asyncio

import aiohttp
import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.tcp_positioncap import TcpPcapSettings
from dranspose.ingesters.zmqsub_contrast import (
    ZmqSubContrastIngester,
    ZmqSubContrastSettings,
)
from dranspose.protocol import (
    EnsembleState,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.worker import Worker, WorkerSettings
from examples.dummy.sw_trig_ingester import SoftTriggerPcapIngester


@pytest.mark.asyncio
async def test_ingester(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pcap: Callable[[int], Coroutine[None, None, None]],
    stream_pkls: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
    tmp_path: pathlib.PurePath,
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
        ZmqSubContrastIngester(
            settings=ZmqSubContrastSettings(
                ingester_streams=[StreamName("contrast")],
                upstream_url=Url("tcp://localhost:5556"),
                ingester_url=Url("tcp://localhost:10000"),
            ),
        )
    )

    ntrig = 20

    await create_ingester(
        SoftTriggerPcapIngester(
            settings=TcpPcapSettings(
                ingester_streams=[StreamName("pcap"), StreamName("dummy")],
                upstream_url=Url("tcp://localhost:8889"),
                ingester_url=Url("tcp://localhost:10001"),
            ),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"contrast", "pcap"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "contrast": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(ntrig)
                ],
                "pcap": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(ntrig)
                ],
                "dummy": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(ntrig)
                ],
            },
        )
        assert resp.status == 200
        await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_pcap(ntrig))

    asyncio.create_task(
        stream_pkls(
            context, 5556, PosixPath("tests/data/contrast-dump.pkls"), 0.001, zmq.PUB
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    context.destroy()

    print(content)
