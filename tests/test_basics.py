import asyncio
import json
import logging
from typing import Awaitable, Callable, Any, Coroutine, Optional
import os

import aiohttp

import pytest
from _pytest.fixtures import FixtureRequest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.protocol import (
    RedisKeys,
    StreamName,
    WorkerName,
)

import redis.asyncio as redis

from dranspose.worker import Worker

from tests.utils import (
    wait_for_controller,
    wait_for_finish,
    set_uniform_sequence,
    monopart_sequence,
    vworker,
)


@pytest.mark.asyncio
async def test_simple(
    request: FixtureRequest,
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, protocol=3)
    await wait_for_controller(
        streams={StreamName("eiger")}, workers={WorkerName("w1")}
    )
    ntrig = 10
    uuid = await set_uniform_sequence(streams={StreamName("eiger")}, ntrig=ntrig)
    updates = await r.xread({RedisKeys.updates(): 0})
    assert "dranspose:controller:updates" in updates
    assert json.loads(updates["dranspose:controller:updates"][-1][-1][1]["data"]) == {
        "mapping_uuid": uuid,
        "parameters_version": {},
        "target_parameters_hash": None,
        "active_streams": ["eiger"],
        "finished": False,
    }
    keys = await r.keys("dranspose:*")
    logging.info("dranspose keys are %s", keys)
    present_keys = {
        "dranspose:controller:updates",
        "dranspose:worker:w1:config",
        f"dranspose:assigned:{uuid}",
        f"dranspose:ready:{uuid}",
        "dranspose:controller_lock",
        "dranspose:reducer:reducer:config",
    }
    if request.config.getoption("rust"):
        present_keys.add("dranspose:ingester:rust-eiger-ingester:config")
    else:
        present_keys.add("dranspose:ingester:eiger-ingester:config")

    assert present_keys.issubset(keys)

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))

    content = await wait_for_finish()

    assert content == {
        "last_assigned": ntrig + 1,
        "completed_events": ntrig + 1,
        "total_events": ntrig + 1,
        "finished": True,
    }
    context.destroy()

    await r.aclose()


@pytest.mark.asyncio
async def test_map(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_small: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
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
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("alba")],
                upstream_url=Url("tcp://localhost:9997"),
                ingester_url=Url("tcp://localhost:10012"),
            ),
        )
    )
    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("slow")],
                upstream_url=Url("tcp://localhost:9996"),
                ingester_url=Url("tcp://localhost:10013"),
            ),
        )
    )

    await wait_for_controller(
        streams={
            StreamName("eiger"),
            StreamName("orca"),
            StreamName("alba"),
            StreamName("slow"),
        }
    )
    async with aiohttp.ClientSession() as session:
        print("startup done")
        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence",
            json=monopart_sequence({
                "eiger": [ [ vworker(2 * i) ] for i in range(1, ntrig) ],
                "orca": [ [ vworker(2 * i + 1) ] for i in range(1, ntrig) ],
                "alba": [ [ vworker(2 * i), vworker(2 * i + 1) ] for i in range(1, ntrig) ],
                "slow": [
                    [ vworker(2 * i), vworker(2 * i + 1) ]
                    if i % 4 == 0
                    else None
                    for i in range(1, ntrig)
                ],
            }),
        )
        assert resp.status == 200

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))
    asyncio.create_task(stream_orca(context, 9998, ntrig - 1))
    asyncio.create_task(stream_small(context, 9997, ntrig - 1))
    asyncio.create_task(stream_small(context, 9996, ntrig // 4))

    content = await wait_for_finish()
    assert content == {
        "last_assigned": ntrig + 1,
        "completed_events": ntrig + 1,
        "total_events": ntrig + 1,
        "finished": True,
    }

    context.destroy()
