import asyncio
import logging
from typing import Callable, Optional, Awaitable, Any

import aiohttp
import pytest
from pydantic import BaseModel

from dranspose.protocol import WorkerName, EnsembleState
from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller


class PayloadParameters(BaseModel):
    roi1: str = "bla"
    file_parameter: str = ""
    file_blob: bytes = b"\x00"
    number: int = 0
    flow: float = 0.0
    isit: bool = False
    complex: tuple[int, int] | None = None
    crazy: dict[str, dict[int, str]] = {}


class ParamWorker:
    parameter_class = PayloadParameters

    def __init__(self, *args, **kwargs) -> None:
        pass

    def process_event(
        self, *args, parameters: PayloadParameters = None, **kwargs
    ) -> None:
        logging.info("worker got params %s", parameters)


class ParamReducer:
    parameter_class = PayloadParameters

    def __init__(self, **kwargs: Any) -> None:
        pass


@pytest.mark.asyncio
async def test_params(
    reducer: Callable[[Optional[str]], Awaitable[None]],
) -> None:
    await reducer("tests.test_reducer_parameters:ParamReducer")

    async with aiohttp.ClientSession() as session:
        par = await session.get("http://localhost:5001/openapi.json")

        logging.info(
            "schema %s", (await par.json())["paths"]["/api/v1/parameters"]["post"]
        )

        par = await session.get("http://localhost:5001/api/v1/parameters")
        assert par.status == 200
        params = PayloadParameters.model_validate_json(await par.content.read())

        logging.info("default params %s", params)

        newparam = PayloadParameters(file_blob=b"\x12" * 100, crazy={"one": {3: "as"}})
        res = await session.post(
            "http://localhost:5001/api/v1/parameters",
            json=newparam.model_dump(mode="json"),
        )
        assert res.status == 200

        par = await session.get("http://localhost:5001/api/v1/parameters")
        assert par.status == 200
        params = PayloadParameters.model_validate_json(await par.content.read())

        assert params == newparam

        # partial update by just sending the changed values
        res = await session.post(
            "http://localhost:5001/api/v1/parameters", json={"roi1": "manual"}
        )
        assert res.status == 200

        par = await session.get("http://localhost:5001/api/v1/parameters")
        assert par.status == 200
        params = PayloadParameters.model_validate_json(await par.content.read())

        newparam.roi1 = "manual"
        assert params == newparam

        res = await session.post(
            "http://localhost:5001/api/v1/parameters",
            json={"crazy": {"alpha": {1: "beta"}}},
        )
        assert res.status == 200

        par = await session.get("http://localhost:5001/api/v1/parameters")
        assert par.status == 200
        params = PayloadParameters.model_validate_json(await par.content.read())

        newparam.crazy = {"alpha": {1: "beta"}}
        assert params == newparam

        res = await session.post(
            "http://localhost:5001/api/v1/parameters",
            json={"file_blob": (b"\x13" * 100).decode()},
        )
        assert res.status == 200

        par = await session.get("http://localhost:5001/api/v1/parameters")
        assert par.status == 200
        params = PayloadParameters.model_validate_json(await par.content.read())

        newparam.file_blob = b"\x13" * 100
        assert params == newparam

        res = await session.post(
            "http://localhost:5001/api/v1/parameters",
            json={"crazy": {"alpha": {"should be int": "beta"}}},
        )
        assert res.status == 422


@pytest.mark.asyncio
async def est_transferclass() -> None:
    logging.info("sig %s", PayloadParameters.__signature__)


@pytest.mark.asyncio
async def est_params_worker(
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[WorkerName | Worker], Awaitable[Worker]],
) -> None:
    await reducer("tests.test_reducer_parameters:ParamReducer")
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="tests.test_reducer_parameters:ParamWorker",
            ),
        )
    )

    await wait_for_controller(workers={WorkerName("w1")})

    await asyncio.sleep(2)

    async with aiohttp.ClientSession() as session:
        res = await session.post(
            "http://localhost:5001/api/v1/parameters",
            json={"crazy": {"alpha": {1: "beta"}}},
        )
        assert res.status == 200

        st = await session.get("http://localhost:5000/api/v1/config")
        conf = EnsembleState.model_validate(await st.json())
        logging.info("state %s", conf)
