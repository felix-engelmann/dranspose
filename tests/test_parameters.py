import asyncio
import logging
import os
import pickle
from pathlib import PosixPath
from typing import Callable, Optional, Awaitable, Any, Coroutine

import aiohttp
import numpy as np
import pytest
import zmq
from pydantic_core import Url


from dranspose.ingester import Ingester
from dranspose.ingesters.zmqsub_contrast import (
    ZmqSubContrastSettings,
    ZmqSubContrastIngester,
)
from dranspose.parameters import ParameterList
from dranspose.protocol import (
    WorkerName,
    StreamName,
    VirtualWorker,
    VirtualConstraint,
    EnsembleState,
)
from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller


@pytest.mark.asyncio
async def test_params(
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

    await wait_for_controller(streams={"contrast"})
    async with aiohttp.ClientSession() as session:
        par = await session.get("http://localhost:5000/api/v1/parameters")
        assert par.status == 200
        params = ParameterList.validate_python(await par.json())

        logging.warning("params %s", params)

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/roi1",
            json=[0, 10],
        )
        assert resp.status == 200

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/additional",
            data=b"asdasd",
        )
        assert resp.status == 200
        hash = await resp.json()

        resp = await session.get(
            "http://localhost:5000/api/v1/parameter/additional",
        )
        assert resp.status == 200
        assert await resp.content.read() == b"asdasd"

        large = b"asd" * 1000
        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/large",
            data=large,
        )
        assert resp.status == 200

        resp = await session.get(
            "http://localhost:5000/api/v1/parameter/large",
        )
        assert resp.status == 200
        assert await resp.content.read() == large

        resp = await session.get(
            "http://localhost:5000/api/v1/parameter/nonexistant",
        )
        assert resp.status == 404

        await asyncio.sleep(3)

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/additional",
            data=b"asdasd",
        )
        assert resp.status == 200

        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/file_parameter",
            data=b"mydirectnumpyarray",
        )
        assert resp.status == 200

        arr = np.ones((10, 10))
        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/file_parameter_file",
            data=pickle.dumps(arr),
        )
        assert resp.status == 200
        hash = await resp.json()

        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        print("state", state)
        while str(state.workers[0].parameters_hash) != hash:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/config")
            state = EnsembleState.model_validate(await st.json())
            logging.warning("got state %s", state)

        ntrig = 20
        mp = {
            "contrast": [
                [VirtualWorker(constraint=VirtualConstraint(i)).model_dump(mode="json")]
                for i in range(ntrig)
            ],
        }
        resp = await session.post(
            "http://localhost:5000/api/v1/mapping",
            json=mp,
        )
        assert resp.status == 200

        context = zmq.asyncio.Context()

        await stream_pkls(
            context, 5556, PosixPath("tests/data/contrast-dump.pkls"), 0.001, zmq.PUB
        )

        st = await session.get("http://localhost:5000/api/v1/progress")
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get("http://localhost:5000/api/v1/progress")
            content = await st.json()

        context.destroy()
