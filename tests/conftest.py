import asyncio
import json
import os
import pickle
import random
from typing import (
    Coroutine,
    AsyncIterator,
    Callable,
    Any,
    Optional,
)

import aiohttp
import numpy as np
import pytest_asyncio
import uvicorn
import zmq
from fastapi import FastAPI
from pydantic import HttpUrl
from pydantic_core import Url

from dranspose.controller import app
from dranspose.ingester import Ingester
from dranspose.protocol import WorkerName
from dranspose.worker import Worker, WorkerSettings
from dranspose.reducer import app as reducer_app
from dranspose.debug_worker import app as debugworker_app
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

    async def _make_worker(name: WorkerName | Worker) -> Worker:
        if not isinstance(name, Worker):
            worker = Worker(WorkerSettings(worker_name=name))
        else:
            worker = name
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
async def reducer(
    tmp_path: Any,
) -> AsyncIterator[Callable[[Optional[str]], Coroutine[None, None, None]]]:
    server_tasks = []

    async def start_reducer(custom: Optional[str] = None) -> None:
        envfile = None
        if custom:
            p = tmp_path / "reducer.env"
            print(p, type(p))
            p.write_text(
                f"""
                REDUCER_CLASS="{custom}"
                """
            )
            envfile = str(p)
        config = uvicorn.Config(
            reducer_app, port=5001, log_level="debug", env_file=envfile
        )
        server = uvicorn.Server(config)
        server_tasks.append((server, asyncio.create_task(server.serve())))
        while server.started is False:
            await asyncio.sleep(0.1)

    yield start_reducer

    for server, task in server_tasks:
        server.should_exit = True
        await task

    await asyncio.sleep(0.1)


@pytest_asyncio.fixture()
async def http_ingester(
    tmp_path: Any,
) -> AsyncIterator[
    Callable[[FastAPI, int, dict[str, Any]], Coroutine[None, None, None]]
]:
    server_tasks = []

    async def start_ingester(
        custom_app: FastAPI, port: int = 5002, env: Optional[dict[str, Any]] = None
    ) -> None:
        envfile = None
        if env:
            p = tmp_path / "http_ingester.env"
            print(p, type(p))
            p.write_text(
                "\n".join(
                    [
                        f"""
                {var.upper()}='{json.dumps(val)}'
                """
                        for var, val in env.items()
                    ]
                )
            )
            envfile = str(p)
        config = uvicorn.Config(
            custom_app, port=port, log_level="debug", env_file=envfile
        )
        server = uvicorn.Server(config)
        server_tasks.append((server, asyncio.create_task(server.serve())))
        while server.started is False:
            await asyncio.sleep(0.1)

    yield start_ingester

    for server, task in server_tasks:
        server.should_exit = True
        await task

    await asyncio.sleep(0.1)


@pytest_asyncio.fixture()
async def debug_worker(
    tmp_path: Any,
) -> AsyncIterator[
    Callable[[Optional[str], Optional[list[str]]], Coroutine[None, None, None]]
]:
    server_tasks = []

    async def start_debug_worker(
        name: Optional[str] = None, tags: Optional[list[str]] = None
    ) -> None:
        envfile = None
        if tags:
            p = tmp_path / "debugworker.env"
            print(p, type(p))
            text = f"""
                WORKER_TAGS='{json.dumps(tags)}'
                """
            if name:
                text += f"""
                    WORKER_NAME = '{name}'
                    """

            p.write_text(text)
            envfile = str(p)
        config = uvicorn.Config(
            debugworker_app, port=5002, log_level="debug", env_file=envfile
        )
        server = uvicorn.Server(config)
        server_tasks.append((server, asyncio.create_task(server.serve())))
        while server.started is False:
            await asyncio.sleep(0.1)
        if tags:  # turns out uvicorn just dumps the envfile in the process env
            del os.environ["WORKER_TAGS"]
            if name:
                del os.environ["WORKER_NAME"]

    yield start_debug_worker

    for server, task in server_tasks:
        server.should_exit = True
        await task

    await asyncio.sleep(0.1)


@pytest_asyncio.fixture
async def stream_eiger() -> Callable[
    [zmq.Context[Any], int, int], Coroutine[Any, Any, None]
]:
    async def _make_eiger(
        ctx: zmq.Context[Any], port: int, nframes: int, frame_time: float = 0.1
    ) -> None:
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
            await asyncio.sleep(frame_time)
        await acq.close()
        await socket.close()

    return _make_eiger


@pytest_asyncio.fixture
async def stream_pkls() -> Callable[
    [
        zmq.Context[Any],
        int,
        os.PathLike[Any] | str,
        float,
        int,
        Optional[int],
        Optional[int],
    ],
    Coroutine[Any, Any, None],
]:
    async def _make_pkls(
        ctx: zmq.Context[Any],
        port: int,
        filename: os.PathLike[Any] | str,
        frame_time: float = 0.1,
        typ: int = zmq.PUSH,
        begin: Optional[int] = None,
        end: Optional[int] = None,
    ) -> None:
        socket: zmq.Socket[Any] = ctx.socket(typ)
        socket.bind(f"tcp://*:{port}")
        if begin is None and end is None:
            for _ in range(3):
                await socket.send_multipart([b"emptyness"])
                await asyncio.sleep(0.1)
        with open(filename, "rb") as f:
            i = 0
            while True:
                try:
                    frames = pickle.load(f)
                    send = True
                    if i < (begin or 0):
                        send = False
                    if end:
                        if i >= end:
                            send = False
                    if send:
                        await socket.send_multipart(frames)
                        await asyncio.sleep(frame_time)
                except EOFError:
                    break

        socket.close()

    return _make_pkls


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


@pytest_asyncio.fixture
async def stream_sardana() -> Callable[[HttpUrl, int], Coroutine[Any, Any, None]]:
    async def _make_sardana(url: HttpUrl, nframes: int) -> None:
        async with aiohttp.ClientSession() as session:
            descurl = HttpUrl.build(
                scheme=url.scheme,
                username=url.username,
                password=url.password,
                host=url.host or "localhost",
                port=url.port,
                path="v1/data_desc",
            )
            recurl = HttpUrl.build(
                scheme=url.scheme,
                username=url.username,
                password=url.password,
                host=url.host or "localhost",
                port=url.port,
                path="v1/record_data",
            )
            endurl = HttpUrl.build(
                scheme=url.scheme,
                username=url.username,
                password=url.password,
                host=url.host or "localhost",
                port=url.port,
                path="v1/record_end",
            )
            st = await session.post(
                str(descurl),
                json={
                    "column_desc": [
                        {
                            "name": "point_nb",
                            "label": "#Pt No",
                            "dtype": "int64",
                            "shape": [],
                        },
                        {
                            "min_value": 0,
                            "max_value": 0.1,
                            "instrument": "",
                            "name": "dummy_mot_01",
                            "label": "dummy_mot_01",
                            "dtype": "float64",
                            "shape": [],
                            "is_reference": True,
                        },
                        {
                            "name": "timestamp",
                            "label": "dt",
                            "dtype": "float64",
                            "shape": [],
                        },
                    ],
                    "ref_moveables": ["dummy_mot_01"],
                    "estimatedtime": -2.6585786096205566,
                    "total_scan_intervals": 2,
                    "starttime": "Mon Apr 17 14:19:35 2023",
                    "title": "burstscan dummy_mot_01 0.0 0.1 2 50",
                    "counters": [
                        "tango://b-v-femtomax-csdb-0.maxiv.lu.se:10000/expchan/oscc_02_seq_ctrl/1",
                        "tango://b-v-femtomax-csdb-0.maxiv.lu.se:10000/expchan/panda_femtopcap_ctrl/4",
                        "tango://b-v-femtomax-csdb-0.maxiv.lu.se:10000/expchan/panda_femtopcap_ctrl/5",
                    ],
                    "scanfile": ["stream.daq", "tests_03.h5"],
                    "scandir": "/data/staff/femtomax/20230413",
                    "serialno": 20338,
                },
            )
            assert st.status == 200

            for frameno in range(nframes):
                st = await session.post(
                    str(recurl),
                    json={
                        "point_nb": frameno,
                        "dummy_mot_01": 0,
                        "timestamp": 0.6104741096496582,
                    },
                )
                assert st.status == 200
                await asyncio.sleep(0.1)
            st = await session.post(str(endurl), json={})
            assert st.status == 200

    return _make_sardana
