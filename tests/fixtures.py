import asyncio
import random
from typing import AsyncGenerator, Coroutine, Awaitable, AsyncIterator, Callable, Any

import numpy as np
import pytest_asyncio
import uvicorn
import zmq
from pydantic_core import Url

from dranspose.controller import app
from dranspose.ingester import Ingester
from dranspose.protocol import WorkerName
from dranspose.worker import Worker
from dranspose.reducer import app as reducer_app
from tests.stream1 import AcquisitionSocket


@pytest_asyncio.fixture()
async def controller() -> AsyncIterator[None]:
    config = uvicorn.Config(app, port=5000, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)
    yield
    server.should_exit = True
    await server_task
    await asyncio.sleep(0.1)


@pytest_asyncio.fixture
async def create_worker() -> AsyncIterator[
    Callable[[WorkerName], Coroutine[None, None, Worker]]
]:
    workers = []

    async def _make_worker(name: WorkerName) -> Worker:
        worker = Worker(name)
        worker_task = asyncio.create_task(worker.run())
        workers.append((worker, worker_task))
        return worker

    yield _make_worker

    for worker, task in workers:
        await worker.close()
        task.cancel()


@pytest_asyncio.fixture
async def create_ingester() -> AsyncIterator[
    Callable[[Ingester], Coroutine[None, None, Ingester]]
]:
    ingesters = []

    async def _make_ingester(inst: Ingester) -> Ingester:
        ingester_task = asyncio.create_task(inst.run())
        ingesters.append((inst, ingester_task))
        return inst

    yield _make_ingester

    for inst, task in ingesters:
        await inst.close()
        task.cancel()


@pytest_asyncio.fixture()
async def reducer() -> AsyncIterator[None]:
    config = uvicorn.Config(reducer_app, port=5001, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)
    yield
    server.should_exit = True
    await server_task
    await asyncio.sleep(0.1)


@pytest_asyncio.fixture
async def stream_eiger() -> Callable[
    [zmq.Context[Any], int, int], Coroutine[Any, Any, None]
]:
    async def _make_eiger(ctx: zmq.Context[Any], port: int, nframes: int) -> None:
        socket = AcquisitionSocket(ctx, Url(f"tcp://*:{port}"))
        acq = await socket.start(filename="")
        width = 1475
        height = 831
        for frameno in range(nframes):
            img = np.zeros((width, height), dtype=np.uint16)
            for _ in range(20):
                img[random.randint(0, width - 1)][
                    random.randint(0, height - 1)
                ] = random.randint(0, 10)
            await acq.image(img, img.shape, frameno)
            await asyncio.sleep(0.1)
        await acq.close()
        await socket.close()

    return _make_eiger


@pytest_asyncio.fixture
async def stream_orca() -> Callable[
    [zmq.Context[Any], int, int], Coroutine[Any, Any, None]
]:
    async def _make_orca(ctx: zmq.Context[Any], port: int, nframes: int) -> None:
        socket = AcquisitionSocket(ctx, Url(f"tcp://*:{port}"))
        acq = await socket.start(filename="")
        width = 2000
        height = 4000
        for frameno in range(nframes):
            img = np.zeros((width, height), dtype=np.uint16)
            for _ in range(20):
                img[random.randint(0, width - 1)][
                    random.randint(0, height - 1)
                ] = random.randint(0, 10)
            await acq.image(img, img.shape, frameno)
            await asyncio.sleep(0.1)
        await acq.close()
        await socket.close()

    return _make_orca


@pytest_asyncio.fixture
async def stream_alba() -> Callable[
    [zmq.Context[Any], int, int], Coroutine[Any, Any, None]
]:
    async def _make_alba(ctx: zmq.Context[Any], port: int, nframes: int) -> None:
        socket = AcquisitionSocket(ctx, Url(f"tcp://*:{port}"))
        acq = await socket.start(filename="")
        val = np.zeros((0,), dtype=np.float64)
        for frameno in range(nframes):
            await acq.image(val, val.shape, frameno)
            await asyncio.sleep(0.1)
        await acq.close()
        await socket.close()

    return _make_alba
