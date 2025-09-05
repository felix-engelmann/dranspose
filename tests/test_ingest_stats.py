import asyncio
import logging
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
    EnsembleState,
)

from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.asyncio
async def test_map(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_worker(WorkerName("w2"))
    await create_worker(WorkerName("w3"))
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("orca")],
                upstream_url=Url("tcp://localhost:9998"),
                ingester_url=Url("tcp://localhost:10011"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("eiger"), StreamName("orca")})
    ntrig = 100
    await set_uniform_sequence({StreamName("eiger")}, ntrig)

    with zmq.asyncio.Context() as context:
        asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))
        content = await wait_for_finish()
        async with aiohttp.ClientSession() as session:
            st = await session.get("http://localhost:5000/api/v1/config")
            conf = EnsembleState.model_validate(await st.json())
            msg = []

            for k in conf.workers + conf.ingesters + [conf.reducer]:
                if k is None:
                    continue
                msg.append(f"{k.name}:{k.processed_events} -- {k.event_rate}")
                if k.name == "eiger-ingester":
                    assert k.processed_events > 0
                elif k.name == "orca-ingester":
                    assert k.processed_events == 0

            logging.info("state is \n%s", "\n".join(msg))

    print(content)

