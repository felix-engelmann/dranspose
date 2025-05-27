import asyncio
import logging
from typing import Optional, Any

import aiohttp

from pydantic_core import Url

from dranspose.protocol import EnsembleState, StreamName, WorkerName, ParameterName


async def wait_for_controller(
    streams: Optional[set[StreamName]] = None,
    workers: Optional[set[WorkerName]] = None,
    parameters: Optional[set[ParameterName]] = None,
    controller: Url = Url("http://localhost:5000"),
) -> EnsembleState:
    async with aiohttp.ClientSession() as session:
        st = await session.get(f"{controller}api/v1/config")
        if streams is None:
            streams = set()
        if workers is None:
            workers = set()
        state = EnsembleState.model_validate(await st.json())
        timeout = 0
        while not (
            streams <= set(state.get_streams()) and workers <= set(state.get_workers())
        ):
            await asyncio.sleep(0.3)
            timeout += 1
            st = await session.get(f"{controller}api/v1/config")
            state = EnsembleState.model_validate(await st.json())
            if timeout < 20:
                logging.debug("queried for components to become available %s", state)
            elif timeout % 4 == 0:
                logging.warning("still waiting for conponents %s", state)
            elif timeout > 40:
                logging.error(
                    "streams %s and workers %s are not available", streams, workers
                )
                raise TimeoutError("failed to bring up components")

        if parameters is None:
            parameters = set()
        for param in parameters:
            resp = await session.get(f"{controller}api/v1/parameter/{param}")
            while resp.status != 200:
                await asyncio.sleep(1)
                logging.warning("waiting for parameter %s", param)
                resp = await session.get(f"{controller}api/v1/parameter/{param}")

        return state


async def wait_for_finish(
    controller: Url = Url("http://localhost:5000"),
) -> dict[str, Any]:
    async with aiohttp.ClientSession() as session:
        st = await session.get(f"{controller}api/v1/progress")
        content = await st.json()
        timeout = 0
        completed_before = -1
        while not content["finished"]:
            await asyncio.sleep(0.3)
            timeout += 1
            st = await session.get(f"{controller}api/v1/progress")
            content = await st.json()
            if content["completed_events"] != completed_before:
                timeout = 0

            if timeout < 20:
                logging.debug("at progress", content)
            elif timeout % 4 == 0:
                logging.warning("stuck at progress %s", content)
            elif timeout > 40:
                logging.error("stalled at progress %s", content)
                raise TimeoutError("processing stalled")
        return content
