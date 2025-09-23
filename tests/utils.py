import asyncio
import logging
from typing import Optional, Any

import aiohttp

from pydantic_core import Url

from dranspose.protocol import (
    EnsembleState,
    StreamName,
    WorkerName,
    ParameterName,
    VirtualWorker,
    VirtualConstraint,
    WorkerTag,
)


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
            timeout = 0
            resp = await session.get(f"{controller}api/v1/parameter/{param}")
            while resp.status != 200:
                await asyncio.sleep(1)
                timeout += 1
                logging.warning("waiting for parameter %s", param)
                resp = await session.get(f"{controller}api/v1/parameter/{param}")
                if timeout > 20:
                    logging.error("parameter %s is not available", param)
                    raise TimeoutError("failed to bring up parameters")
            logging.info("parameter %s is available", param)
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


def vworker(
    constraint: Optional[int] = None, tags: Optional[set[str]] = None
) -> dict[str, Any]:
    """this is just syntactic sugar to define a virtual worker"""
    vw = VirtualWorker()
    if constraint is not None:
        vw.constraint = VirtualConstraint(constraint)
    if tags is not None:
        vw.tags = {WorkerTag(t) for t in tags}
    return vw.model_dump(mode="json")


def monopart_sequence(mapping: dict[str, Any]) -> dict[str, Any]:
    return {"parts": {"main": mapping}, "sequence": ["main"]}


def uniform_sequence(streams: set[StreamName], ntrig: int) -> dict[str, Any]:
    """
    QUIRK ALERT: note that this actually expects ntrig-1 triggers,
    since it starts at 1.
    """
    return monopart_sequence(
        {
            stream_name: [[vworker(i)] for i in range(1, ntrig)]
            for stream_name in streams
        }
    )


async def set_sequence(sequence: dict[Any, Any]) -> str:
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5000/api/v1/sequence/", json=sequence
        )
        if resp.status != 200:
            print("sent", sequence)
            raise RuntimeError(f"bad response when setting sequence: {resp.status=}")
        uuid = await resp.json()
        return uuid


async def set_uniform_sequence(streams: set[StreamName], ntrig: int) -> str:
    """
    QUIRK ALERT: note that this actually expects ntrig-1 triggers,
    since it starts at 1.
    """
    sequence = uniform_sequence(streams, ntrig)
    return await set_sequence(sequence)
