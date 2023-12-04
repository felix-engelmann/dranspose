import os
import pathlib
import pickle
from pathlib import PosixPath

import asyncio
from typing import Awaitable, Callable, Any, Coroutine, Never, Optional, Generator
import zmq.asyncio

import aiohttp
import pytest
from pydantic_core import Url

from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_contrast import (
    StreamingContrastIngester,
    StreamingContrastSettings,
)
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
from dranspose.ingesters.streaming_xspress3 import (
    StreamingXspressIngester,
    StreamingXspressSettings,
)
from dranspose.protocol import (
    EnsembleState,
    RedisKeys,
    StreamName,
    WorkerName,
    VirtualWorker,
    VirtualConstraint,
)
from dranspose.worker import Worker, WorkerSettings

from tests.fixtures import (
    controller,
    reducer,
    create_worker,
    create_ingester,
    stream_pkls,
)


@pytest.mark.asyncio
async def test_reduction(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_pkls: Callable[
        [zmq.Context[Any], int, os.PathLike[Any], float, int],
        Coroutine[Any, Any, Never],
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
    print(p_contrast)
    await create_ingester(
        StreamingContrastIngester(
            name=StreamName("contrast"),
            settings=StreamingContrastSettings(
                upstream_url=Url("tcp://localhost:5556"),
                ingester_url=Url("tcp://localhost:10000"),
                dump_path=p_contrast,
            ),
        )
    )
    p_xspress = tmp_path / "xspress_ingest.pkls"
    await create_ingester(
        StreamingXspressIngester(
            name=StreamName("xspress3"),
            settings=StreamingXspressSettings(
                upstream_url=Url("tcp://localhost:9999"),
                ingester_url=Url("tcp://localhost:10001"),
                dump_path=p_xspress,
            ),
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        while {"contrast", "xspress3"} - set(state.get_streams()) != set():
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())

        resp = await session.post(
            "http://localhost:5000/api/v1/parameters/json",
            json={"roi1": [0, 10]},
        )
        assert resp.status == 200
        uuid = await resp.json()

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
        uuid = await resp.json()

    context = zmq.asyncio.Context()

    asyncio.create_task(
        stream_pkls(
            context, 9999, PosixPath("tests/data/xspress3-dump.pkls"), 0.001, zmq.PUB
        )
    )
    asyncio.create_task(
        stream_pkls(
            context, 5556, PosixPath("tests/data/contrast-dump.pkls"), 0.001, zmq.PUB
        )
    )

    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

        st = await session.get("http://localhost:5001/api/v1/result/pickle")
        content = await st.content.read()
        result = pickle.loads(content)
        print("content", result)
        assert len(result["map"].keys()) == ntrig

    context.destroy()

    print(content)
