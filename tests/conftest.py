import asyncio
import gzip
import json
import logging
import os
import queue
import random
import socket
import struct
import threading
import time
from asyncio import StreamReader, StreamWriter, Task
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError
from typing import (
    Coroutine,
    AsyncIterator,
    Callable,
    Any,
    Optional,
)

import aiohttp
import cbor2
import numpy as np
import pytest
import pytest_asyncio
import uvicorn
import zmq
from _pytest.fixtures import FixtureRequest
from _pytest.logging import LogCaptureFixture
from aiohttp import ClientConnectionError
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
from dranspose.workload_generator import create_app
from tests.stream1 import AcquisitionSocket

import redis.asyncio as redis

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
    parser.addoption(
        "--observe",
        action="store_true",
        dest="observe",
        default=False,
        help="enable sampling redis content",
    )


class ErrorLoggingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.error_found = False

    def emit(self, record: logging.LogRecord) -> None:
        # Check if the log message starts with "ERROR"
        if record.levelname == "ERROR":
            if hasattr(record, "message") and (
                "Unclosed connection" in record.message
            ):  # aiohttp benign error
                return
            self.error_found = True


@pytest_asyncio.fixture(autouse=True)
async def fail_on_error_log(
    request: FixtureRequest, caplog: LogCaptureFixture
) -> AsyncIterator[None]:
    handler = ErrorLoggingHandler()
    logging.getLogger().addHandler(handler)

    yield

    # After the test runs, check if any "ERROR" log was encountered
    logging.getLogger().removeHandler(handler)
    fail_on_log_err = request.node.get_closest_marker("allow_errors_in_log")
    if handler.error_found and fail_on_log_err is None:
        pytest.fail("Test failed due to log message starting with 'ERROR'")


def is_port_available(port, host="0.0.0.0"):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
            return True
        except OSError:
            return False


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
    timeout = 5
    while (timeout := timeout - 1) > 0 and not (available := is_port_available(5000)):
        logging.warning("needed to wait for port to become available")
        await asyncio.sleep(1)

    if not available:
        pytest.fail("could not stop controller within 5 seconds")


@dataclass
class AsyncDistributed:
    instance: DistributedService
    task: Task[Any]

    async def stop(self) -> None:
        await self.instance.close()
        await cancel_and_wait(self.task)


@dataclass
class ThreadDistributed:
    instance: DistributedService
    thread: threading.Thread
    queue: Any

    async def stop(self) -> None:
        self.queue.put("stop")
        self.thread.join()


@dataclass
class ExternalDistributed:
    instance: DistributedService
    process: asyncio.subprocess.Process
    log_tasks: list[Task[Any]]

    async def stop(self) -> None:
        logging.warning("stopping process")
        self.process.terminate()
        await self.process.wait()
        logging.warning("stopping log task")
        for t in self.log_tasks:
            t.cancel()


@pytest_asyncio.fixture
async def create_worker() -> (
    AsyncIterator[Callable[[WorkerName], Coroutine[None, None, Worker]]]
):
    workers: list[AsyncDistributed | ThreadDistributed] = []

    async def _make_worker(name: WorkerName | Worker, threaded: bool = False) -> Worker:
        if not isinstance(name, Worker):
            worker = Worker(WorkerSettings(worker_name=name))
        else:
            worker = name

        if threaded:
            q: Any = queue.Queue()
            p = threading.Thread(target=worker.sync_run, args=(q,), daemon=True)
            p.start()
            workers.append(ThreadDistributed(instance=worker, thread=p, queue=q))
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
    ingesters: list[AsyncDistributed | ThreadDistributed | ExternalDistributed] = []

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

    async def _make_ingester(inst: Ingester, threaded: bool = False) -> Ingester:
        if request.config.getoption("rust"):
            logging.info("replace ingester with rust")
            if isinstance(inst, ZmqPullSingleIngester):
                logging.info("use settings %s", inst._streaming_single_settings)
                if os.path.exists("/bin/fast_ingester"):
                    binary_path = "/bin/fast_ingester"
                else:
                    binary_path = "./perf/target/debug/fast_ingester"
                    logging.warning("using debug ingester")
                proc = await asyncio.create_subprocess_exec(
                    binary_path,
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
                outerr = asyncio.create_task(
                    forward_pipe(proc.stderr, inst._streaming_single_settings)
                )
                ingesters.append(
                    ExternalDistributed(
                        instance=inst, process=proc, log_tasks=[output, outerr]
                    )
                )
                return inst
        if threaded:
            q: Any = queue.Queue()
            p = threading.Thread(target=inst.sync_run, args=(q,), daemon=True)
            p.start()
            ingesters.append(ThreadDistributed(instance=inst, thread=p, queue=q))
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
        app = create_app()

        config = uvicorn.Config(app, port=port, log_level="debug")
        server = uvicorn.Server(config=config)
        task = asyncio.create_task(server.serve())

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    st = await session.get(f"http://localhost:{port}/api/v1/finished")
                    if st.status == 200:
                        break
                    else:
                        await asyncio.sleep(0.5)
                except ClientConnectionError:
                    await asyncio.sleep(0.5)

        server_tasks.append((task, server))

    yield start_generator

    for task, server in server_tasks:
        server.should_exit = True
        await task

    await asyncio.sleep(0.1)


@pytest_asyncio.fixture
async def stream_eiger() -> (
    Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]]
):
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
            extra = {"timestamps": {"dummy": datetime.now(timezone.utc).isoformat()}}
            await acq.image(img, img.shape, frameno, extra_fields=extra)
            await asyncio.sleep(frame_time)
        await acq.close()
        await socket.close()

    return _make_eiger


@pytest_asyncio.fixture
async def stream_cbors() -> Callable[
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
    async def _make_cbors(
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
        if begin is not None and end is not None:
            assert end > begin
        base, ext = os.path.splitext(filename)
        if ext == ".gz":
            _, ext = os.path.splitext(base)
            assert (
                "cbor" in ext
            ), f"gz file does not appear to be a cbor file: {filename}"
            f = gzip.open(filename, "rb")
        else:
            assert "cbor" in ext, f"file does not appear to be a cbor file: {filename}"
            f = open(filename, "rb")
        i = 0
        while True:
            try:
                frames = cbor2.load(f)
                send = True
                if i < (begin or 0):
                    send = False
                if end:
                    if i >= end:
                        send = False
                i += 1
                if send:
                    await socket.send_multipart(frames)
                    # logging.debug("created message %s", frames)
                    await asyncio.sleep(frame_time)
            except (cbor2.CBORDecodeEOF, EOFError):
                logging.info("all messages read from cbor, total: %d", i)
                break
        assert i > 0, f"CBOR file [{filename}] had no frames"
        f.close()
        socket.close()

    return _make_cbors


@pytest_asyncio.fixture
async def stream_orca() -> (
    Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]]
):
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
async def stream_small() -> (
    Callable[[zmq.Context[Any], int | str, int], Coroutine[Any, Any, None]]
):
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
async def stream_small_xrd() -> (
    Callable[[zmq.Context[Any], int | str, int], Coroutine[Any, Any, None]]
):
    async def _make_xrd(
        ctx: zmq.Context[Any], port: int | str, nframes: int, frame_time: float = 0.1
    ) -> None:
        if type(port) is str:
            socket = AcquisitionSocket(ctx, Url(port))
        else:
            socket = AcquisitionSocket(ctx, Url(f"tcp://*:{port}"))
        acq = await socket.start(filename="output/xrd.h5")
        start = time.perf_counter()
        for frameno in range(nframes):
            base = np.zeros((10, 10), dtype=np.uint16)
            for x in range(base.shape[0]):
                for y in range(base.shape[1]):
                    base[x, y] = max(
                        np.sin(
                            (
                                (x - base.shape[0] / 2) ** 2
                                + (y - base.shape[1] / 2) ** 2
                            )
                            / (3 + frameno)
                        )
                        * 10
                        + 10
                        + random.randrange(-2, 2),
                        0,
                    )
            await acq.image(base, base.shape, frameno)
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

    return _make_xrd


@pytest_asyncio.fixture
async def time_beacon() -> (
    Callable[[zmq.Context[Any], int, int], Coroutine[Any, Any, None]]
):
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
async def stream_pcap() -> (
    AsyncIterator[Callable[[int, int, int], Coroutine[None, None, None]]]
):
    server_tasks = []

    async def _make_pcap(
        nframes: int = 10, port: int = 8889, repetitions: int = 1
    ) -> None:
        async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
            request = (await reader.read(255)).decode("utf8")
            if request.strip() != "":
                logging.error("bad initial hello %s", request)
            writer.write(b"OK\n")
            await writer.drain()
            for i in range(repetitions):
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

                writer.write(f"END {nframes} Disarmed\n".encode("utf8"))
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


def sample_table_to_md(table: dict[str, Any], filename: str) -> None:
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        col_order = sorted(table.keys())  # , key=lambda name: name[::-1])
        logging.info("column order is %s", col_order)
        f.write(f'| {" | ".join(col_order)} |\n')
        f.write(f'| {" | ".join(["---" for _ in table])} |\n')
        last_line: tuple[Any, ...] = tuple([""] * len(table))
        for line in zip(*[table[col] for col in col_order]):
            if line == last_line:
                continue
            change_line = []
            for old, new in zip(last_line, line):
                if new is None:
                    change_line.append("absent")
                elif old == new:
                    change_line.append('--"--')
                else:
                    change = new
                    olddata = None
                    if isinstance(old, str):
                        try:
                            olddata = json.loads(old)
                        except JSONDecodeError:
                            pass
                    if isinstance(new, str):
                        try:
                            data = json.loads(new)
                            diff = data
                            change = ""
                            if olddata is not None:
                                diff = {}
                                for key, item in data.items():
                                    if key in olddata and olddata[key] == item:
                                        continue
                                    diff[key] = item
                                change = "**changed**<br>"
                            change += (
                                json.dumps(diff, indent=2)
                                .replace("\n", "<br>")
                                .replace(" ", "&nbsp;")
                            )
                        except JSONDecodeError:
                            pass
                    elif isinstance(new, bytes):
                        change = str(new[:30]) + f" ({len(new)} bytes)"
                    elif isinstance(new, list):
                        change = ""
                        for entry in new:
                            change += "**" + entry[0] + "**<br>"
                            data = json.loads(entry[1]["data"])
                            pretty = (
                                json.dumps(data, indent=2)
                                .replace("\n", "<br>")
                                .replace(" ", "&nbsp;")
                            )
                            change += pretty + "<br>"
                    change_line.append(change)
            f.write(f'| {" | ".join(map(str, change_line))} |\n')
            last_line = line
    logging.info("written redis observation to %s", filename)


def sample_table_to_html(
    table: dict[str, str | list[tuple[str, str]] | bytes], filename: str
) -> None:
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        col_order = sorted(table.keys())  # , key=lambda name: name[::-1])
        logging.info("column order is %s", col_order)
        f.write(
            """
        <html>
        <head>
        <style>
        th {
          position: sticky;
          top: 50px;
          background: white;
        }
        </style>
        </head>
        <body style="padding-top:50px;">
        <table>
        <thead>
          <tr>
        """
        )
        for col in col_order:
            f.write(f"<th>{col}</th>")
        f.write(
            """  </tr>
        </thead>
        <tbody>
        """
        )
        last_line: tuple[Any, ...] = tuple([""] * len(table))
        for line in zip(*[table[col] for col in col_order]):
            if line == last_line:
                continue
            change_line = []
            for old, new in zip(last_line, line):
                if new is None:
                    change_line.append("absent")
                elif old == new:
                    change_line.append('--"--')
                else:
                    change = new
                    olddata = None
                    if isinstance(old, str):
                        try:
                            olddata = json.loads(old)
                        except JSONDecodeError:
                            pass
                    if isinstance(new, str):
                        try:
                            data = json.loads(new)
                            diff = data
                            change = ""
                            if olddata is not None:
                                diff = {}
                                for key, item in data.items():
                                    if key in olddata and olddata[key] == item:
                                        continue
                                    diff[key] = item
                                change = "<b>changed</b><br>"
                            change += "<pre>"
                            change += json.dumps(diff, indent=2)
                            change += "</pre>"
                        except JSONDecodeError:
                            pass
                    elif isinstance(new, bytes):
                        change = str(new[:30]) + f" ({len(new)} bytes)"
                    elif isinstance(new, list):
                        change = ""
                        for entry in new:
                            change += "<b>" + entry[0] + "</b><br><pre>"
                            data = json.loads(entry[1]["data"])
                            pretty = json.dumps(data, indent=2)
                            change += pretty + "</pre><br>"
                    change_line.append(change)
            f.write("<tr>")
            for el in change_line:
                f.write(f"<td>{str(el)}</td>")
            f.write("<tr>")
            last_line = line
        f.write(
            """
        </tbody>
        </table></body></html>"""
        )
    logging.info("written redis observation to %s", filename)


async def sample_redis(filename: str) -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, protocol=3)
    r_raw = redis.from_url(redis_url, decode_responses=False, protocol=3)
    table: dict[str, Any] = {}
    try:
        tick = 0
        start_ids: dict[str, str] = {}
        while True:
            keys = await r.keys("dranspose:*")
            logging.debug("all redis keys %s", keys)
            for key in keys:
                if key.startswith("dranspose:parameters:"):
                    data = await r_raw.get(key)
                elif any(
                    [
                        match in key
                        for match in [
                            "dranspose:ready:",
                            "dranspose:controller:updates",
                            "dranspose:assigned:",
                        ]
                    ]
                ):
                    start = start_ids.get(key, "-")
                    if start != "-":
                        start = "(" + start
                    data = await r.xrange(key, start, "+")
                    if len(data) > 0:
                        start_ids[key] = data[-1][0]
                    logging.debug("data is %s", data)
                else:
                    data = await r.get(key)
                if key not in table:
                    table[key] = [None] * tick
                table[key].append(data)
                logging.debug("key %s contains %s", key, data)
            for missing in table:
                if missing not in keys:
                    table[missing].append(None)
            await asyncio.sleep(0.1)
            tick += 1
    except asyncio.exceptions.CancelledError:
        # sample_table_to_md(table, filename)
        sample_table_to_html(table, filename)
        await r.aclose()


@pytest_asyncio.fixture(autouse=True)
async def observe_redis(request: FixtureRequest) -> AsyncIterator[None]:
    if request.config.getoption("observe"):
        filename = (
            "redis-observations/"
            + request.node.nodeid.replace("/", ".").replace("::", "--")
            + ".html"
        )
        t = asyncio.create_task(sample_redis(filename))
        yield
        t.cancel()
        await t
    else:
        yield
