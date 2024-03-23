import asyncio
from typing import Awaitable, Callable, Coroutine, Optional

import aiohttp

import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.tcp_positioncap import TcpPcapIngester, TcpPcapSettings
from dranspose.protocol import (
    EnsembleState,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker, WorkerSettings


@pytest.mark.asyncio
async def test_panda_pcap(
    stream_pcap: Callable[[int], Coroutine[None, None, None]],
) -> None:
    ntrig = 10
    asyncio.create_task(stream_pcap(ntrig - 1))
    await asyncio.sleep(0.5)
    reader, writer = await asyncio.open_connection("localhost", 8889)
    writer.write(b"\n")
    data = await reader.readline()
    assert data == b"OK\n"
    while data := await reader.readline():
        if data.startswith(b"END"):
            break
    writer.close()
    await writer.wait_closed()

    await asyncio.sleep(1)


@pytest.mark.asyncio
async def test_pcapingester(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pcap: Callable[[int], Coroutine[None, None, None]],
) -> None:
    await reducer(None)
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.parser.positioncap:PcapWorker",
            ),
        )
    )
    await create_ingester(
        TcpPcapIngester(
            settings=TcpPcapSettings(
                ingester_streams=[StreamName("pcap")],
                upstream_url=Url("tcp://localhost:8889"),
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

        ntrig = 10
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

    asyncio.create_task(stream_pcap(ntrig - 1))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

    print(content)
