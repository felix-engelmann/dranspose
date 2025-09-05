import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Any, Coroutine, Optional

import aiohttp
import cbor2

import numpy as np
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
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)

from dranspose.worker import Worker
from tests.utils import (
    wait_for_controller,
    wait_for_finish,
    set_uniform_sequence,
    monopart_sequence,
)


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

    await wait_for_controller(
        streams={
            StreamName("eiger"),
            StreamName("orca"),
            StreamName("alba"),
            StreamName("slow"),
        }
    )
    async with aiohttp.ClientSession() as session:
        p_prefix = tmp_path / "dump_"
        logging.info("prefix is %s encoded: %s", p_prefix,
                     str(p_prefix).encode("utf8"))

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/dump_prefix",
            data=str(p_prefix).encode("utf8"),
        )
        assert resp.status == 200

        ntrig = 10
        payload = monopart_sequence({
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
        })
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence",
            json=payload
        )
        assert resp.status == 200
        uuid = await resp.json()

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

    await wait_for_controller(streams={StreamName("xrd")})
    async with aiohttp.ClientSession() as session:
        p_prefix = tmp_path / "dump_"
        logging.info("prefix is %s encoded: %s", p_prefix,
                     str(p_prefix).encode("utf8"))

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/dump_prefix",
            data=str(p_prefix).encode("utf8"),
        )
        assert resp.status == 200

    ntrig = 10
    await set_uniform_sequence({StreamName("xrd")}, ntrig)

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_small_xrd(context, 9999, ntrig - 1))

    content = await wait_for_finish()

    assert content == {
        "last_assigned": ntrig + 1,
        "completed_events": ntrig + 1,
        "total_events": ntrig + 1,
        "finished": True,
    }
    context.destroy()


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_dump_map_and_parameters(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
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

    await wait_for_controller(streams={StreamName("xrd")})
    async with aiohttp.ClientSession() as session:
        p_prefix = tmp_path / "dump_"
        logging.info("prefix is %s encoded: %s", p_prefix,
                     str(p_prefix).encode("utf8"))

        pars = {
            "dump_prefix": str(p_prefix).encode("utf8"),
            "test_string": "spam".encode("utf8"),
            "test_int": "42".encode("utf8"),
        }
        for name, data in pars.items():
            resp = await session.post(
                f"http://localhost:5000/api/v1/parameter/{name}",
                data=data,
            )
            assert resp.status == 200

    ntrig = 10
    uuid = await set_uniform_sequence({StreamName("xrd")}, ntrig)
    context = zmq.asyncio.Context()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/mapping")
        mapping = await st.json()
        logging.debug("mapping %s", mapping)

        with open(f"{p_prefix}mapping-{uuid}.json", "rb") as f:
            # logging.info(f"Content of mapping {f.read()}")
            dumped_mapping = json.load(f)
            logging.debug("dumped_mapping %s", dumped_mapping)
        assert mapping == dumped_mapping

    with open(f"{p_prefix}parameters-{uuid}.json", "rb") as f:
        dumped_pars = json.load(f)
        logging.debug("dumped_pars %s", dumped_pars)
        for p in dumped_pars:
            assert p["data"].encode("utf8") == pars[p["name"]]

    context.destroy()


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_dump_bin_parameters(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
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

    await wait_for_controller(streams={StreamName("xrd")})
    async with aiohttp.ClientSession() as session:
        p_prefix = tmp_path / "dump_"
        logging.info("prefix is %s encoded: %s", p_prefix, str(p_prefix).encode("utf8"))

        pars = {
            "dump_prefix": str(p_prefix).encode("utf8"),
            "test_null": np.zeros((2, 2)).tobytes(),
        }
        for name, data in pars.items():
            resp = await session.post(
                f"http://localhost:5000/api/v1/parameter/{name}",
                data=data,
            )
            assert resp.status == 200

    ntrig = 10
    uuid = await set_uniform_sequence({StreamName("xrd")}, ntrig)

    context = zmq.asyncio.Context()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/mapping")
        mapping = await st.json()
        logging.debug("mapping %s", mapping)

        with open(f"{p_prefix}mapping-{uuid}.json", "rb") as f:
            # logging.info(f"Content of mapping {f.read()}")
            dumped_mapping = json.load(f)
            logging.debug("dumped_mapping %s", dumped_mapping)
        assert mapping == dumped_mapping

    with open(f"{p_prefix}parameters-{uuid}.cbor", "rb") as f:
        pars_dict = {}
        dumped_pars = cbor2.load(f)
        logging.debug("dumped_pars %s", dumped_pars)
        for p in dumped_pars:
            pars_dict[p["name"]] = p["data"]
        assert pars_dict["dump_prefix"].encode("utf8") == pars["dump_prefix"]
        assert pars_dict["test_null"] == pars["test_null"]

    context.destroy()


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_dump_and_not_dump(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_small_xrd: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    tmp_path: Any,
) -> None:
    dump_path = tmp_path / "first_run.cbors"
    # Set up services
    await reducer(None)
    await create_worker(WorkerName("w1"))
    ing = await create_ingester(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("xrd")],
                upstream_url=Url("tcp://localhost:9999"),
                ingester_url=Url("tcp://localhost:10010"),
                dump_path=dump_path,
            ),
        )
    )

    # Create mapping
    ntrig = 10
    await wait_for_controller(streams={StreamName("xrd")})

    context = zmq.asyncio.Context()

    # Run first scan
    await set_uniform_sequence({StreamName("xrd")}, ntrig)
    asyncio.create_task(stream_small_xrd(context, 9999, ntrig - 1))
    await wait_for_finish()

    ing._ingester_settings.dump_path = None
    time_between_scans = datetime.now(timezone.utc)

    # Run second scan
    await set_uniform_sequence({StreamName("xrd")}, ntrig)
    asyncio.create_task(stream_small_xrd(context, 9999, ntrig - 1))
    await wait_for_finish()

    # Check dump
    with open(dump_path, "rb") as f:
        evs = []
        while True:
            try:
                dat = cbor2.load(f, tag_hook=message_tag_hook)
                if isinstance(dat, InternalWorkerMessage):
                    evs.append(dat.event_number)
                    assert dat.created_at < time_between_scans
            except EOFError:
                break

    assert evs == list(range(0, ntrig + 1))

    context.destroy()
