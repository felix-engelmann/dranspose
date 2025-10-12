import logging
import os
import pickle
from pathlib import PosixPath

import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Optional
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
)
from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller, wait_for_finish, set_uniform_sequence


@pytest.mark.asyncio
async def test_short_scans(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_cbors: Callable[
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

    ntrig = 30
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/roi1",
            json=[0, 10],
        )
        assert resp.status == 200
        await resp.json()
    await set_uniform_sequence({StreamName("contrast"), StreamName("xspress3")}, ntrig)

    context = zmq.asyncio.Context()

    asyncio.create_task(
        stream_cbors(
            context,
            9999,
            PosixPath("tests/data/xspress3mini-dump20.cbors"),
            0.001,
            zmq.PUB,
        )
    )
    asyncio.create_task(
        stream_cbors(
            context, 5556, PosixPath("tests/data/contrast-dump.cbors"), 0.001, zmq.PUB
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        timeout = 0
        while content["completed_events"] < 22:
            await asyncio.sleep(0.3)
            timeout += 1
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()
            if timeout < 20:
                logging.debug("at progress", content)
            elif timeout % 4 == 0:
                logging.warning("stuck at progress %s", content)
            elif timeout > 40:
                logging.error("stalled at progress %s", content)
                raise TimeoutError("processing stalled")

        st = await session.get("http://localhost:5001/api/v1/result/pickle")
        content = await st.content.read()
        result = pickle.loads(content)
        assert result == {
            "map": {
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
            },
            "version": {
                "commit_hash": "1a474d1455f0d6c6113a22179b8fff1e98325916",
                "branch_name": "main",
                "timestamp": "2024-04-25T08:07:59+02:00",
                "repository_url": "https://gitlab.maxiv.lu.se/DanMAX/pipeline/drp-tomo-buffer",
            },
        }
        assert len(result["map"].keys()) == 20

    for _ in range(2):
        await stream_cbors(
            context,
            9999,
            PosixPath("tests/data/xspress3mini-dump20.cbors"),
            0.001,
            zmq.PUB,
        )

    # second scan
    ntrig = 20

    await set_uniform_sequence({StreamName("contrast"), StreamName("xspress3")}, ntrig)

    await stream_cbors(
        context,
        9999,
        PosixPath("tests/data/xspress3mini-dump20.cbors"),
        0.001,
        zmq.PUB,
        begin=10,  # type: ignore[call-arg]
    )

    asyncio.create_task(
        stream_cbors(
            context,
            9999,
            PosixPath("tests/data/xspress3mini-dump20.cbors"),
            0.001,
            zmq.PUB,
        )
    )
    asyncio.create_task(
        stream_cbors(
            context, 5556, PosixPath("tests/data/contrast-dump.cbors"), 0.001, zmq.PUB
        )
    )

    await wait_for_finish()

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5001/api/v1/result/pickle")
        content = await st.content.read()
        result = pickle.loads(content)
        assert result == {
            "map": {
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
            },
            "version": {
                "commit_hash": "1a474d1455f0d6c6113a22179b8fff1e98325916",
                "branch_name": "main",
                "timestamp": "2024-04-25T08:07:59+02:00",
                "repository_url": "https://gitlab.maxiv.lu.se/DanMAX/pipeline/drp-tomo-buffer",
            },
        }
        assert len(result["map"].keys()) == ntrig

    context.destroy()
