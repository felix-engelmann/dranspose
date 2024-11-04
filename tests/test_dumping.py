import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp
import cbor2

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url

from dranspose.event import InternalWorkerMessage, message_tag_hook
from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.protocol import (
    EnsembleState,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_dump(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_small: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    tmp_path: Any,
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))
    await create_worker(WorkerName("w2"))
    await create_worker(WorkerName("w3"))

    p_eiger = tmp_path / "eiger_dump.cbors"
    print(p_eiger, type(p_eiger))

    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
                dump_path=p_eiger,
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

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        logging.debug("content %s", state)
        while {"eiger", "orca", "alba", "slow"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        logging.info("startup done")

        p_prefix = tmp_path / "dump_"
        logging.info("prefix is %s encoded: %s", p_prefix, str(p_prefix).encode("utf8"))

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/dump_prefix",
            data=str(p_prefix).encode("utf8"),
        )
        assert resp.status == 200

        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "eiger": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(2 * i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(1, ntrig)
                ],
                "orca": [
                    [
                        VirtualWorker(
                            constraint=VirtualConstraint(2 * i + 1)
                        ).model_dump(mode="json")
                    ]
                    for i in range(1, ntrig)
                ],
                "alba": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(2 * i)).model_dump(
                            mode="json"
                        ),
                        VirtualWorker(
                            constraint=VirtualConstraint(2 * i + 1)
                        ).model_dump(mode="json"),
                    ]
                    for i in range(1, ntrig)
                ],
                "slow": [
                    [
                        VirtualWorker(constraint=VirtualConstraint(2 * i)).model_dump(
                            mode="json"
                        ),
                        VirtualWorker(
                            constraint=VirtualConstraint(2 * i + 1)
                        ).model_dump(mode="json"),
                    ]
                    if i % 4 == 0
                    else None
                    for i in range(1, ntrig)
                ],
            },
        )
        assert resp.status == 200
        uuid = await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))
    asyncio.create_task(stream_orca(context, 9998, ntrig - 1))
    asyncio.create_task(stream_small(context, 9997, ntrig - 1))
    asyncio.create_task(stream_small(context, 9996, ntrig // 4))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

        assert content == {
            "last_assigned": ntrig + 1,
            "completed_events": ntrig + 1,
            "total_events": ntrig + 1,
            "finished": True,
        }

    # read dump
    with open(p_eiger, "rb") as f:
        evs = []
        while True:
            try:
                dat = cbor2.load(f, tag_hook=message_tag_hook)
                if isinstance(dat, InternalWorkerMessage):
                    evs.append(dat.event_number)
            except EOFError:
                break

    assert evs == list(range(0, ntrig + 1))

    # read prefix dump
    with open(f"{p_prefix}orca-ingester-{uuid}.cbors", "rb") as f:
        evs = []
        while True:
            try:
                before = datetime.now(timezone.utc)
                dat = cbor2.load(f, tag_hook=message_tag_hook)
                if isinstance(dat, InternalWorkerMessage):
                    evs.append(dat.event_number)
                    assert (
                        dat.created_at < before
                    ), "timestamp was not created when loading the data from file but read"
            except EOFError:
                break

    assert evs == list(range(0, ntrig + 1))

    context.destroy()


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_dump_xrd(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_small_xrd: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    tmp_path: Any,
) -> None:
    await reducer(None)
    await create_worker(WorkerName("w1"))

    await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("xrd")],
                upstream_url=Url("tcp://localhost:9999"),
                ingester_url=Url("tcp://localhost:10010"),
            ),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        logging.debug("content %s", state)
        while {"xrd"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        logging.info("startup done")

        p_prefix = tmp_path / "dump_"
        logging.info("prefix is %s encoded: %s", p_prefix, str(p_prefix).encode("utf8"))

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/dump_prefix",
            data=str(p_prefix).encode("utf8"),
        )
        assert resp.status == 200

        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "xrd": [
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

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_small_xrd(context, 9999, ntrig - 1))

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

        assert content == {
            "last_assigned": ntrig + 1,
            "completed_events": ntrig + 1,
            "total_events": ntrig + 1,
            "finished": True,
        }
    context.destroy()
