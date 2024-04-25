import asyncio
import json
import logging
import multiprocessing
import os
import pickle
import random
import struct
import time
from asyncio import StreamReader, StreamWriter, Task
from dataclasses import dataclass
from multiprocessing import Process
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
from _pytest.fixtures import FixtureRequest
from fastapi import FastAPI
from pydantic import HttpUrl
from pydantic_core import Url

from dranspose.controller import app
from dranspose.distributed import DistributedService
from dranspose.helpers.utils import cancel_and_wait
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.ingesters.zmqpull_single import ZmqPullSingleIngester
from dranspose.protocol import WorkerName
from dranspose.worker import Worker, WorkerSettings
from dranspose.reducer import app as reducer_app
from dranspose.debug_worker import app as debugworker_app
from dranspose.workload_generator import app as generator_app
from tests.stream1 import AcquisitionSocket

from pytest import Parser

os.environ["BUILD_META_FILE"] = "tests/data/build_git_meta.json"


def pytest_addoption(parser: Parser) -> None:
    parser.addoption(
        "--rust",
        action="store_true",
        dest="rust",
        default=False,
        help="enable rust decorated tests",
    )
    parser.addoption(
        "--k8s",
        action="store_true",
        dest="k8s",
        default=False,
        help="enable k8s remote tests",
    )


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


@dataclass
class AsyncDistributed:
    instance: DistributedService
    task: Task[Any]

    async def stop(self) -> None:
        await self.instance.close()
        await cancel_and_wait(self.task)


@dataclass
class ProcessDistributed:
    instance: DistributedService
    process: multiprocessing.Process
    queue: Any

    async def stop(self) -> None:
        self.queue.put("stop")
        self.process.join()


@dataclass
class ExternalDistributed:
    instance: DistributedService
    process: asyncio.subprocess.Process
    log_task: Task[Any]

    async def stop(self) -> None:
        logging.warning("stopping process")
        self.process.terminate()
        await self.process.wait()
        logging.warning("stopping log task")
        self.log_task.cancel()


@pytest_asyncio.fixture
async def create_worker() -> AsyncIterator[
    Callable[[WorkerName], Coroutine[None, None, Worker]]
]:
    workers: list[AsyncDistributed | ProcessDistributed] = []

    async def _make_worker(
        name: WorkerName | Worker, subprocess: bool = False
    ) -> Worker:
        if not isinstance(name, Worker):
            worker = Worker(WorkerSettings(worker_name=name))
        else:
            worker = name

        if subprocess:
            q: Any = multiprocessing.Queue()
            p = Process(target=worker.sync_run, args=(q,), daemon=True)
            p.start()
            workers.append(ProcessDistributed(instance=worker, process=p, queue=q))
        else:
            worker_task = asyncio.create_task(worker.run())
            workers.append(AsyncDistributed(instance=worker, task=worker_task))

        return worker

    yield _make_worker

    for wo in workers:
        await wo.stop()


@pytest_asyncio.fixture
async def create_ingester(
    request: FixtureRequest,
) -> AsyncIterator[Callable[[Ingester], Coroutine[None, None, Ingester]]]:
    ingesters: list[AsyncDistributed | ProcessDistributed | ExternalDistributed] = []

    async def forward_pipe(
        out: StreamReader | None, settings: IngesterSettings
    ) -> None:
        if out is None:
            return
        while True:
            try:
                data = await out.readline()
                line = data.decode("ascii").rstrip()
                if len(line) > 0:
                    fn = logging.error
                    parts = line.split("]")
                    if len(parts) > 1:
                        if "INFO" in parts[0]:
                            fn = logging.info
                        elif "DEBUG" in parts[0]:
                            fn = logging.debug
                        # line = "]".join(parts[1:])
                    fn("rust_%s: %s", settings.ingester_streams[0], line)
                else:
                    break
            except Exception as e:
                logging.error("outputing broke %s", e.__repr__())
                break

    async def _make_ingester(inst: Ingester, subprocess: bool = False) -> Ingester:
        if request.config.getoption("rust"):
            logging.warning("replace ingester with rust")
            if isinstance(inst, ZmqPullSingleIngester):
                logging.warning("ues settings %s", inst._streaming_single_settings)
                proc = await asyncio.create_subprocess_exec(
                    "./perf/target/debug/fast_ingester",
                    "--stream",
                    inst._streaming_single_settings.ingester_streams[0],
                    "--upstream-url",
                    str(inst._streaming_single_settings.upstream_url),
                    "--ingester-url",
                    str(inst._streaming_single_settings.ingester_url),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                output = asyncio.create_task(
                    forward_pipe(proc.stdout, inst._streaming_single_settings)
                )
                output = asyncio.create_task(
                    forward_pipe(proc.stderr, inst._streaming_single_settings)
                )
                ingesters.append(
                    ExternalDistributed(instance=inst, process=proc, log_task=output)
                )
                return inst
        if subprocess:
            q: Any = multiprocessing.Queue()
            p = Process(target=inst.sync_run, args=(q,), daemon=True)
            p.start()
            ingesters.append(ProcessDistributed(instance=inst, process=p, queue=q))
        else:
            ingester_task = asyncio.create_task(inst.run())
            ingesters.append(AsyncDistributed(instance=inst, task=ingester_task))
        return inst

    yield _make_ingester

    for ing in ingesters:
        await ing.stop()


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
        if custom:
            del os.environ["REDUCER_CLASS"]

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


@pytest_asyncio.fixture()
async def workload_generator(
    tmp_path: Any,
) -> AsyncIterator[Callable[[int], Coroutine[None, None, None]]]:
    server_tasks = []

    async def start_generator(port: int = 5003) -> None:
        config = uvicorn.Config(generator_app, port=port, log_level="debug")
        server = uvicorn.Server(config)
        server_tasks.append((server, asyncio.create_task(server.serve())))
        while server.started is False:
            await asyncio.sleep(0.1)

    yield start_generator

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
async def stream_small() -> Callable[
    [zmq.Context[Any], int | str, int], Coroutine[Any, Any, None]
]:
    async def _make_alba(
        ctx: zmq.Context[Any], port: int | str, nframes: int, frame_time: float = 0.1
    ) -> None:
        if type(port) is str:
            socket = AcquisitionSocket(ctx, Url(port))
        else:
            socket = AcquisitionSocket(ctx, Url(f"tcp://*:{port}"))
        acq = await socket.start(filename="")
        val = np.zeros((10000,), dtype=np.float64)
        start = time.perf_counter()
        for frameno in range(nframes):
            await acq.image(val, val.shape, frameno)
            if frame_time:
                await asyncio.sleep(frame_time)
            if frameno % 1000 == 0:
                end = time.perf_counter()
                logging.info(
                    "send 1000 packets took %s: %lf p/s",
                    end - start,
                    1000 / (end - start),
                )
                start = end
        await acq.close()
        await socket.close()

    return _make_alba


@pytest_asyncio.fixture
async def time_beacon() -> Callable[
    [zmq.Context[Any], int, int], Coroutine[Any, Any, None]
]:
    async def _make_time(
        ctx: zmq.Context[Any],
        port: int,
        nframes: int,
        frame_time: float = 0.1,
        flags: int = 0,
    ) -> None:
        sout = ctx.socket(zmq.PUSH)
        sout.bind(f"tcp://*:{port}")
        start = time.perf_counter()
        for i in range(nframes):
            data = struct.pack(">d", time.perf_counter())
            try:
                await sout.send(data, flags=flags)
            except zmq.Again:
                pass
            if frame_time is not None:
                await asyncio.sleep(frame_time)
            if i % 1000 == 0:
                end = time.perf_counter()
                logging.info(
                    "send 1000 time packets took %s: %lf p/s",
                    end - start,
                    1000 / (end - start),
                )
                start = end
        sout.close(linger=0)

    return _make_time


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


@pytest_asyncio.fixture
async def stream_pcap() -> AsyncIterator[
    Callable[[int, int], Coroutine[None, None, None]]
]:
    server_tasks = []

    async def _make_pcap(nframes: int = 10, port: int = 8889) -> None:
        async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
            request = (await reader.read(255)).decode("utf8")
            if request.strip() != "":
                logging.error("bad initial hello %s", request)
            writer.write(b"OK\n")
            await writer.drain()

            header = b"""arm_time: 2023-12-19T07:51:12.754Z
missed: 0
process: Scaled
format: ASCII
fields:
 PCAP.BITS0 uint32 Value
 INENC1.VAL double Min scale: 1 offset: 0 units:
 INENC1.VAL double Max scale: 1 offset: 0 units:
 PCAP.TS_TRIG double Value scale: 8e-09 offset: 0 units: s
 INENC1.VAL double Mean scale: 1 offset: 0 units:
 SFP3_SYNC_IN.POS1 double Mean scale: 1 offset: 0 units: (null)

"""
            writer.write(header)
            await writer.drain()

            data = b""" 301989857 0 0 0.32175968 0 592910924.7
 33554401 0 0 0.43175968 0 592910919
 33554401 0 0 0.54175968 0 592910930.9
 33554401 0 0 0.65175968 0 592910930.2
 301989857 0 0 0.76175968 0 592910946.1
 301989857 0 0 0.87175968 0 592910969.6
 301989857 0 0 0.98175968 0 592911000.4
 301989857 0 0 1.09175968 0 592911008.9
 301989857 0 0 1.20175968 0 592910962.2
 301989857 0 0 1.31175968 0 592910916.7
 301989857 0 0 1.42175968 0 592910829.8""".split(
                b"\n"
            )

            for i in range(nframes):
                writer.write(data[i % 11] + b"\n")
                await writer.drain()
                await asyncio.sleep(0.1)

            writer.write(f"END {nframes} Disarmed".encode("utf8"))
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle_client, "localhost", port)
        server_tasks.append(asyncio.create_task(server.serve_forever()))

        logging.warning("started server %s", server.is_serving())
        # async with server:
        #    await server.serve_forever()

    yield _make_pcap

    for task in server_tasks:
        await cancel_and_wait(task)


multiscan_trace = """

OK
arm_time: 2023-12-19T08:49:51.103Z
missed: 0
process: Scaled
format: ASCII
fields:
 PCAP.BITS0 uint32 Value
 INENC1.VAL double Min scale: 1 offset: 0 units:
 INENC1.VAL double Max scale: 1 offset: 0 units:
 PCAP.TS_TRIG double Value scale: 8e-09 offset: 0 units: s
 INENC1.VAL double Mean scale: 1 offset: 0 units:
 SFP3_SYNC_IN.POS1 double Mean scale: 1 offset: 0 units: (null)

 301989857 0 0 0.290445128 0 592659802.5
 33554401 0 0 0.400445128 0 592659790.2
 301989857 0 0 0.510445128 0 592659777.6
 301989857 0 0 0.620445128 0 592659776.1
 301989857 0 0 0.730445128 0 592659780.1
 301989857 0 0 0.840445128 0 592659786.7
 301989857 0 0 0.950445128 0 592659778.1
 301989857 0 0 1.060445128 0 592659753.3
 33554401 0 0 1.170445128 0 592659740.2
 301989857 0 0 1.280445128 0 592659740.5
 301989857 0 0 1.390445128 0 592659745.6
END 11 Disarmed
arm_time: 2023-12-19T08:49:59.911Z
missed: 0
process: Scaled
format: ASCII
fields:
 PCAP.BITS0 uint32 Value
 INENC1.VAL double Min scale: 1 offset: 0 units:
 INENC1.VAL double Max scale: 1 offset: 0 units:
 PCAP.TS_TRIG double Value scale: 8e-09 offset: 0 units: s
 INENC1.VAL double Mean scale: 1 offset: 0 units:
 SFP3_SYNC_IN.POS1 double Mean scale: 1 offset: 0 units: (null)

 301989857 0 0 0.280505096 0 592659819.6
 301989857 0 0 0.390505096 0 592659807.4
 301989857 0 0 0.500505096 0 592659823.6
 33554401 0 0 0.610505096 0 592659827.9
 33554401 0 0 0.720505096 0 592659835.1
 301989857 0 0 0.830505096 0 592659817.4
 301989857 0 0 0.940505096 0 592659828.3
 301989857 0 0 1.050505096 0 592659823.2
 301989857 0 0 1.160505096 0 592659851.4
 33554401 0 0 1.270505096 0 592659838.7
 33554401 0 0 1.380505096 0 592659842.7
END 11 Disarmed"""
