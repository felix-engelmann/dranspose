import asyncio
import gzip
import json
import logging
import pickle
import shutil
import threading
from typing import Awaitable, Callable, Any, Coroutine, Optional, IO

import aiohttp
import cbor2
import h5pyd
import numpy as np

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
)

from dranspose.replay import replay
from dranspose.worker import Worker
from tests.utils import wait_for_controller, wait_for_finish, vworker, monopart_sequence


async def dump_data(
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_small: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    tmp_path: Any,
):
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
        logging.info("prefix is %s encoded: %s", p_prefix, str(p_prefix).encode("utf8"))

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/dump_prefix",
            data=str(p_prefix).encode("utf8"),
        )
        assert resp.status == 200

        ntrig = 10
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence",
            json=monopart_sequence(
                {
                    "eiger": [[vworker(2 * i)] for i in range(1, ntrig)],
                    "orca": [[vworker(2 * i + 1)] for i in range(1, ntrig)],
                    "alba": [
                        [vworker(2 * i), vworker(2 * i + 1)] for i in range(1, ntrig)
                    ],
                    "slow": [
                        [vworker(2 * i), vworker(2 * i + 1)] if i % 4 == 0 else None
                        for i in range(1, ntrig)
                    ],
                }
            ),
        )
        assert resp.status == 200
        uuid = await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context, 9999, ntrig - 1))
    asyncio.create_task(stream_orca(context, 9998, ntrig - 1))
    asyncio.create_task(stream_small(context, 9997, ntrig - 1))
    asyncio.create_task(stream_small(context, 9996, ntrig // 4))

    await wait_for_finish()

    context.destroy()

    return p_eiger, p_prefix, uuid


def generate_params(tmp_path):
    par_file = tmp_path / "parameters.json"

    with open(par_file, "w") as f:
        json.dump([{"name": "roi1", "data": "[10,20]"}], f)
    return par_file


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_replay(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_small: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    tmp_path: Any,
) -> None:
    p_eiger, p_prefix, uuid = await dump_data(
        reducer,
        create_worker,
        create_ingester,
        stream_eiger,
        stream_orca,
        stream_small,
        tmp_path,
    )
    # read dump

    par_file = generate_params(tmp_path)

    replay(
        "examples.dummy.worker:FluorescenceWorker",
        "examples.dummy.reducer:FluorescenceReducer",
        [p_eiger, f"{p_prefix}orca-ingester-{uuid}.cbors"],
        None,
        par_file,
    )


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_replay_gzip(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_small: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    tmp_path: Any,
) -> None:
    p_eiger, p_prefix, uuid = await dump_data(
        reducer,
        create_worker,
        create_ingester,
        stream_eiger,
        stream_orca,
        stream_small,
        tmp_path,
    )

    with open(f"{p_prefix}orca-ingester-{uuid}.cbors", "rb") as f_in:
        with gzip.open(f"{p_prefix}orca-ingester-{uuid}.cbors.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    stop_event = threading.Event()
    done_event = threading.Event()

    par_file = generate_params(tmp_path)

    thread = threading.Thread(
        target=replay,
        args=(
            "tests.aux_payloads:TestWorker",
            "tests.aux_payloads:TestReducer",
            [p_eiger, f"{p_prefix}orca-ingester-{uuid}.cbors.gz"],
            None,
            par_file,
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    thread.start()

    logging.info("keep webserver alive")
    done_event.wait()
    logging.info("replay done")

    def work_pre() -> None:
        ntrig = 10
        f = h5pyd.File("http://localhost:5010/", "r")
        logging.info("file %s", list(f.keys()))
        logging.warning("map %s", f["results"])
        logging.warning("res keys %s", list(f["results"].keys()))
        assert list(f.keys()) == ["results", "parameters"]
        assert list(f["results"].keys()) == list(map(str, range(ntrig + 1)))
        logging.info("streams in ev 5: %s", list(f["results/5"].keys()))
        logging.info("header of eiger %s", list(f["results/5/eiger"].keys()))
        detector = f["results/5/eiger"]
        assert detector["htype"][()] == b"image"
        assert detector["frame"][()] == 4
        assert np.array_equal(detector["shape"][:], [1475, 831])
        assert detector["type"][()] == b"uint16"
        assert detector["compression"][()] == b"none"
        assert detector["msg_number"][()] == 5
        assert "dummy" in list(detector["timestamps"].keys())
        detector = f["results/3/orca"]
        assert detector["htype"][()] == b"image"
        assert detector["frame"][()] == 2
        assert np.array_equal(detector["shape"][:], [2000, 4000])
        assert detector["type"][()] == b"uint16"
        assert detector["compression"][()] == b"none"
        assert detector["msg_number"][()] == 3

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work_pre)

    logging.info("shut down server")
    stop_event.set()

    thread.join()
    await asyncio.sleep(0.1)
    logging.info("thread joined")


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_replay_pklparam(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_small: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    tmp_path: Any,
) -> None:
    p_eiger, p_prefix, uuid = await dump_data(
        reducer,
        create_worker,
        create_ingester,
        stream_eiger,
        stream_orca,
        stream_small,
        tmp_path,
    )

    bin_file = tmp_path / "binparams.pkl"

    fb: IO[bytes]
    with open(bin_file, "wb") as fb:
        arr = np.ones((10, 10))
        pickle.dump(
            [
                {"name": "roi1", "data": "[10,20]"},
                {"name": "file_parameter_file", "data": pickle.dumps(arr)},
            ],
            fb,
        )

    stop_event = threading.Event()
    done_event = threading.Event()

    thread = threading.Thread(
        target=replay,
        args=(
            "examples.dummy.worker:FluorescenceWorker",
            "examples.dummy.reducer:FluorescenceReducer",
            [p_eiger, f"{p_prefix}orca-ingester-{uuid}.cbors"],
            None,
            bin_file,
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    thread.start()

    logging.info("keep webserver alive")
    done_event.wait()
    logging.info("replay done")

    def work_first() -> None:
        f = h5pyd.File("http://localhost:5010/", "r")
        logging.info("file %s", list(f.keys()))
        logging.info("map %s", list(f["map"].keys()))
        assert list(f.keys()) == ["map"]

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work_first)

    logging.info("shut down server")
    stop_event.set()

    thread.join()
    await asyncio.sleep(0.1)
    logging.info("thread joined")


@pytest.mark.skipif("config.getoption('rust')", reason="rust does not support dumping")
@pytest.mark.asyncio
async def test_replay_cborparam(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_orca: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    stream_small: Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]],
    tmp_path: Any,
) -> None:
    p_eiger, p_prefix, uuid = await dump_data(
        reducer,
        create_worker,
        create_ingester,
        stream_eiger,
        stream_orca,
        stream_small,
        tmp_path,
    )

    cbor_file = tmp_path / "binparams.cbor"

    with open(cbor_file, mode="wb") as fb:
        arr = np.ones((10, 10))
        cbor2.dump(
            [
                {"name": "roi1", "data": "[10,20]"},
                {"name": "file_parameter_file", "data": pickle.dumps(arr)},
            ],
            fb,
        )

    stop_event = threading.Event()
    done_event = threading.Event()

    thread = threading.Thread(
        target=replay,
        args=(
            "examples.dummy.worker:FluorescenceWorker",
            "examples.dummy.reducer:FluorescenceReducer",
            [p_eiger, f"{p_prefix}orca-ingester-{uuid}.cbors"],
            None,
            cbor_file,
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    thread.start()

    logging.info("keep webserver alive")
    done_event.wait()
    logging.info("replay done")

    def work_second() -> None:
        f = h5pyd.File("http://localhost:5010/", "r")
        logging.info("file %s", list(f.keys()))
        logging.info("map %s", list(f["map"].keys()))
        assert list(f.keys()) == ["map"]

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work_second)

    logging.info("shut down server")
    stop_event.set()

    thread.join()
    await asyncio.sleep(0.1)
    logging.info("thread joined")
