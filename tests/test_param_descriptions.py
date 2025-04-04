import logging
from typing import Callable, Optional, Awaitable

import aiohttp
import pytest
from pydantic_core import Url


from dranspose.ingester import Ingester
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.parameters import ParameterList, BinaryParameter, StrParameter
from dranspose.protocol import WorkerName, StreamName, ParameterName
from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller


@pytest.mark.asyncio
async def test_params(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
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
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )

    await wait_for_controller(streams={StreamName("eiger")})
    async with aiohttp.ClientSession() as session:
        par = await session.get("http://localhost:5000/api/v1/parameters")
        params = ParameterList.validate_python(await par.json())

        logging.warning("params %s", params)
        assert params == [
            StrParameter(
                name=ParameterName("dump_prefix"),
                description="Prefix to dump ingester values",
                dtype="str",
            ),
            StrParameter(
                name=ParameterName("file_parameter"), description=None, dtype="str"
            ),
            BinaryParameter(
                name=ParameterName("file_parameter_file"),
                description=None,
                dtype="binary",
            ),
            BinaryParameter(
                name=ParameterName("other_file_parameter"),
                description=None,
                dtype="binary",
            ),
            StrParameter(
                name=ParameterName("roi1"), description=None, dtype="str", default="bla"
            ),
            StrParameter(
                name=ParameterName("string_parameter"), description=None, dtype="str"
            ),
        ]
