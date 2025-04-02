import logging
import os
import pathlib
import pickle
from pathlib import PosixPath
import h5pyd

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
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_finish, wait_for_controller


@pytest.mark.asyncio
async def test_reduction(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
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
    p_contrast = tmp_path / "contrast_ingest.pkls"

    await create_ingester(
        ZmqSubContrastIngester(
            settings=ZmqSubContrastSettings(
                ingester_streams=[StreamName("contrast")],
                upstream_url=Url("tcp://localhost:5556"),
                ingester_url=Url("tcp://localhost:10000"),
                dump_path=p_contrast,
            ),
        )
    )
    p_xspress = tmp_path / "xspress_ingest.pkls"
    await create_ingester(
        ZmqSubXspressIngester(
            settings=ZmqSubXspressSettings(
                ingester_streams=[StreamName("xspress3")],
                upstream_url=Url("tcp://localhost:9999"),
                ingester_url=Url("tcp://localhost:10001"),
                dump_path=p_xspress,
            ),
        )
    )

    await wait_for_controller(streams={"contrast", "xspress3"})
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/roi1",
            json=[0, 10],
        )
        assert resp.status == 200
        await resp.json()

        ntrig = 20
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
                "xspress3": [
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

    content = await wait_for_finish()
    assert content == {
        "last_assigned": ntrig + 2,
        "completed_events": ntrig + 2,
        "total_events": ntrig + 2,
        "finished": True,
    }

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
        assert (
            result["version"]["commit_hash"]
            == "1a474d1455f0d6c6113a22179b8fff1e98325916"
        )

    def work() -> None:
        f = h5pyd.File("http://localhost:5001/", "r")
        logging.info("file %s", list(f.keys()))
        logging.info("map %s", list(f["map"].keys()))
        logging.info("version %s", f["version/commit_hash"][()])
        assert (
            f["version"]["commit_hash"][()]
            == b"1a474d1455f0d6c6113a22179b8fff1e98325916"
        )

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    context.destroy()
