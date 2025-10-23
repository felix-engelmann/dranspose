import logging
from typing import Callable, Optional, Awaitable, Any

import aiohttp
import pytest
from pydantic import BaseModel


class PayloadParameters(BaseModel):
    roi1: str = "bla"
    file_parameter: str = ""
    file_blob: bytes = b"\x00"
    number: int = 0
    flow: float = 0.0
    isit: bool = False
    complex: tuple[int, int] | None = None
    crazy: dict[str, dict[int, str]] = {}


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

        newparam = PayloadParameters(file_blob=b"\x12" * 100)
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
