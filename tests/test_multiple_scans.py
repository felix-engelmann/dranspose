import logging
import os
from pathlib import PosixPath
import h5pyd

import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional

import numpy as np
import zmq.asyncio

import aiohttp
import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.zmqsub_contrast import (
    ZmqSubContrastIngester,
    ZmqSubContrastSettings,
)
from dranspose.ingesters.zmqsub_xspress3 import (
    ZmqSubXspressIngester,
    ZmqSubXspressSettings,
)
from dranspose.protocol import (
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller, wait_for_finish


@pytest.mark.asyncio
async def test_multiple_scans(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pkls: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
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
    await create_ingester(
        ZmqSubXspressIngester(
            settings=ZmqSubXspressSettings(
                ingester_streams=[StreamName("xspress3")],
                upstream_url=Url("tcp://localhost:9999"),
                ingester_url=Url("tcp://localhost:10001"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("contrast"), StreamName("xspress3")})
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/roi1",
            json=[0, 10],
        )
        assert resp.status == 200
        await resp.json()

        ntrig = 20
        mp = {
            "contrast": [
                [VirtualWorker(constraint=VirtualConstraint(i)).model_dump(mode="json")]
                for i in range(ntrig)
            ],
            "xspress3": [
                [VirtualWorker(constraint=VirtualConstraint(i)).model_dump(mode="json")]
                for i in range(ntrig)
            ],
        }
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json=mp,
        )
        assert resp.status == 200
        await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(
        stream_pkls(
            context,
            9999,
            PosixPath("tests/data/xspress3mini-dump20.pkls"),
            0.001,
            zmq.PUB,
        )
    )
    asyncio.create_task(
        stream_pkls(
            context, 5556, PosixPath("tests/data/contrast-dump.pkls"), 0.001, zmq.PUB
        )
    )

    await wait_for_finish()

    def work() -> None:
        f = h5pyd.File("http://localhost:5001/", "r")
        logging.info(
            f"file {list(f.keys())}",
        )
        logging.info("version %s", f["version"])
        should = {
            (-2.004051863, -2.002903037): {"roi1": 81},
            (-2.001404022, -0.9992748245): {"roi1": 76},
            (-2.000941193, 0.001238407776): {"roi1": 67},
            (-2.000656853, 1.001692484): {"roi1": 49},
            (-2.000562814, 2.0019763): {"roi1": 69},
            (-0.6658021229, -2.002587864): {"roi1": 97},
            (-0.6664982812, -0.9992643584): {"roi1": 75},
            (-0.6666898134, 0.001223421204): {"roi1": 58},
            (-0.6666922578, 1.001631271): {"roi1": 81},
            (-0.6666616732, 2.001930725): {"roi1": 77},
            (0.6676852429, -2.002412156): {"roi1": 91},
            (0.6666162969, -0.9992055423): {"roi1": 81},
            (0.6664269385, 0.001214749016): {"roi1": 76},
            (0.6663550388, 1.00170782): {"roi1": 67},
            (0.6663102034, 2.002018839): {"roi1": 49},
            (2.001407847, -2.002363463): {"roi1": 69},
            (2.000183463, -0.9992309104): {"roi1": 97},
            (1.999963432, 0.001258902382): {"roi1": 75},
            (1.999884878, 1.001690624): {"roi1": 58},
            (1.999828501, 2.001959397): {"roi1": 81},
        }
        assert np.array_equal(f["map"]["x"][:], [x[0] for x in should.keys()])
        assert np.array_equal(f["map"]["y"][:], [x[1] for x in should.keys()])
        assert np.array_equal(
            f["map"]["data"]["roi1"], [y["roi1"] for y in should.values()]
        )

        assert (
            f["version"]["commit_hash"][()]
            == b"1a474d1455f0d6c6113a22179b8fff1e98325916"
        )
        assert f["version"]["branch_name"][()] == b"main"
        assert f["version"]["timestamp"][()] == b"2024-04-25T08:07:59+02:00"
        assert (
            f["version"]["repository_url"][()]
            == b"https://gitlab.maxiv.lu.se/DanMAX/pipeline/drp-tomo-buffer"
        )

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    for _ in range(2):
        await stream_pkls(
            context,
            9999,
            PosixPath("tests/data/xspress3mini-dump20.pkls"),
            0.001,
            zmq.PUB,
        )

    # second scan
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json=mp,
        )
        assert resp.status == 200
        await resp.json()

    await stream_pkls(
        context,
        9999,
        PosixPath("tests/data/xspress3mini-dump20.pkls"),
        0.001,
        zmq.PUB,
        begin=30,  # type: ignore[call-arg]
    )

    asyncio.create_task(
        stream_pkls(
            context,
            9999,
            PosixPath("tests/data/xspress3mini-dump20.pkls"),
            0.001,
            zmq.PUB,
        )
    )
    asyncio.create_task(
        stream_pkls(
            context, 5556, PosixPath("tests/data/contrast-dump.pkls"), 0.001, zmq.PUB
        )
    )

    await wait_for_finish()

    def work2() -> None:
        f = h5pyd.File("http://localhost:5001/", "r")
        logging.info(
            f"file {list(f.keys())}",
        )
        assert len(f["map"]["x"][:]) == ntrig

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work2)

    context.destroy()
