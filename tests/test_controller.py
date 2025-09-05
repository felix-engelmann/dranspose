from typing import Callable, Awaitable

import aiohttp
import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.protocol import (
    VirtualWorker,
    VirtualConstraint,
    StreamName,
)
from tests.utils import wait_for_controller, uniform_sequence, set_uniform_sequence


@pytest.mark.asyncio
async def test_status(controller: None) -> None:
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/status")
        await st.json()
        assert st.status == 200


@pytest.mark.asyncio
async def test_stream_not_available(controller: None) -> None:
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/status")
        assert st.status == 200
    ntrig = 10
    async with aiohttp.ClientSession() as session:
        seq = uniform_sequence({StreamName("eiger")}, ntrig=ntrig)
        resp = await session.post("http://localhost:5000/api/v1/sequence", json=seq)
        assert resp.status == 400
        response = await resp.json()
        assert {"detail": "streams {'eiger'} not available"} == response


@pytest.mark.asyncio
async def test_not_enough_workers(
    controller: None, create_ingester: Callable[[Ingester], Awaitable[Ingester]]
) -> None:
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )
    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
        ntrig = 10
        sequence = uniform_sequence({StreamName("eiger")}, ntrig)
        resp = await session.post("http://localhost:5000/api/v1/sequence/", json=sequence)
        assert resp.status == 400
        response = await resp.json()
        assert {"detail": "only 0 workers available, but 1 required"} == response
