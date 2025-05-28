import asyncio
import logging
from typing import Callable, Optional, Awaitable, Sequence, Any

import aiohttp
import pytest

from dranspose.parameters import ParameterList, ParameterBase, StrParameter
from dranspose.protocol import WorkerName, ParameterName
from dranspose.worker import Worker, WorkerSettings


class ParamWorker:
    def __init__(self, **kwargs: Any) -> None:
        pass

    @staticmethod
    def describe_parameters() -> Sequence[ParameterBase]:
        params = [
            StrParameter(name=ParameterName("roi1"), default="bla"),
            StrParameter(name=ParameterName("file_parameter")),
        ]
        return params


@pytest.mark.asyncio
async def test_default_params(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
) -> None:
    await reducer(None)
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="tests.test_default_parameters:ParamWorker",
            ),
        )
    )

    async with aiohttp.ClientSession() as session:
        par = await session.get("http://localhost:5000/api/v1/parameters")
        assert par.status == 200
        params = ParameterList.validate_python(await par.json())

        logging.warning("params %s", params)

        await asyncio.sleep(3)  # the default check runs every 2 seconds

        resp = await session.get(
            "http://localhost:5000/api/v1/parameter/roi1",
        )
        assert resp.status == 200
        assert await resp.content.read() == b"bla"

        resp = await session.get(
            "http://localhost:5000/api/v1/parameter/file_parameter",
        )
        assert resp.status == 200
        assert await resp.content.read() == b""
