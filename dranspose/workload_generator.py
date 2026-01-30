import asyncio
import json
import logging
import random
import time
from asyncio import Task
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from json import JSONDecodeError
from typing import AsyncGenerator, Any, Tuple

import numpy as np
import psutil
import zmq.asyncio
from fastapi import FastAPI, Request
from psutil._ntuples import snicaddr, snetio, snicstats
from pydantic import BaseModel, Field, field_serializer
from pydantic_core import Url

from dranspose.helpers.utils import cancel_and_wait, done_callback
from tests.stream1 import AcquisitionSocket

logger = logging.getLogger(__name__)


class SocketSpec(BaseModel):
    type: int = zmq.PUSH
    port: int = 9999


class ConnectSpec(BaseModel):
    type: int = zmq.PULL
    url: Url = Url("tcp://127.0.0.1:9999")


class WorkloadSpec(BaseModel):
    number: int = 1
    time: float = 0.1
    shape: Tuple[int, int] = (10, 10)
    filename: str = ""


class Statistics(BaseModel):
    snapshots: deque[tuple[float, int, dict[str, snetio]]] = Field(
        default_factory=lambda: deque(maxlen=100)
    )
    fps: float = 0
    packets: int = 0
    measured: float = 0
    deltas: list[float] = []

    @field_serializer("snapshots", mode="wrap")
    def serialize_sn(
        self, snapshots: deque[tuple[float, int, dict[str, snetio]]], _info: Any
    ) -> list[tuple[float, int, dict[str, snetio]]]:
        return list(snapshots)


class NetworkConfig(BaseModel):
    addresses: dict[str, list[snicaddr]]
    stats: dict[str, snicstats]


class WorkloadGenerator:
    def __init__(self, **kwargs: Any) -> None:
        self.context = zmq.asyncio.Context()
        self.out_socket: AcquisitionSocket | None = None
        self.in_socket: zmq.asyncio.Socket | None = None
        self.task: Task[Any] | None = None
        self.stat = Statistics()
        logger.info("Workload generator initialised")

    def finished(self) -> bool:
        if self.task is None:
            return True
        return self.task.done()

    async def calc_stat(self) -> None:
        start = time.time()
        before_packets = self.stat.packets
        while True:
            await asyncio.sleep(0.5)
            after = time.time()
            num = self.stat.packets
            ctr = psutil.net_io_counters(pernic=True)
            self.stat.measured = after
            self.stat.fps = (num - before_packets) / (after - start)
            self.stat.snapshots.append((after, num, ctr))
            logger.debug("calculated stats %s fps", self.stat.fps)
            start = after
            before_packets = num

    async def open_socket(self, spec: SocketSpec) -> None:
        await self.close_socket()
        self.stat = Statistics()
        self.out_socket = AcquisitionSocket(
            self.context, Url(f"tcp://*:{spec.port}"), typ=spec.type
        )
        logger.info("socket opened to %s is %s", spec, self.out_socket)

    async def sink_packets(self) -> None:
        if not isinstance(self.in_socket, zmq.Socket):
            raise Exception("only sink packets from connected socket")
        while True:
            pkt = await self.in_socket.recv_multipart(copy=False)
            try:
                header = json.loads(pkt[0].bytes)
                ts = header.get("timestamps", {})
                now = datetime.now(timezone.utc)
                delta = now - min(map(lambda x: datetime.fromisoformat(x), ts.values()))
                logger.debug("delta is %s", delta.total_seconds())
                self.stat.deltas.append(delta.total_seconds())
            except (JSONDecodeError, ValueError):
                pass
            logger.debug("sinked packet %s", pkt)
            self.stat.packets += 1

    async def connect_socket(self, spec: ConnectSpec) -> None:
        await self.close_socket()
        self.stat = Statistics()
        self.in_socket = self.context.socket(spec.type)
        self.in_socket.connect(str(spec.url))
        if spec.type == zmq.SUB:
            self.in_socket.setsockopt(zmq.SUBSCRIBE, b"")
        logger.info("socket connected to %s", spec)
        self.task = asyncio.create_task(self.sink_packets())
        self.task.add_done_callback(done_callback)

    async def send_packets(self, spec: WorkloadSpec) -> None:
        logger.info("send packets from socket %s", self.out_socket)
        if self.out_socket is None:
            raise Exception("must open socket before sending packets")
        if not isinstance(self.out_socket, AcquisitionSocket):
            raise Exception("must be AcquisitionSocket to send, is %s", self.out_socket)
        logger.info("sending packets on socket %s", self.out_socket)
        acq = await self.out_socket.start(filename=spec.filename)
        logger.info("sending packets %s", spec)
        self.stat.packets += 1
        width = spec.shape[0]
        height = spec.shape[1]
        img = np.zeros((width, height), dtype=np.uint16)
        for _ in range(20):
            img[random.randint(0, width - 1)][
                random.randint(0, height - 1)
            ] = random.randint(0, 10)
        for frameno in range(spec.number):
            extra = {
                "timestamps": {"generator": datetime.now(timezone.utc).isoformat()}
            }
            await acq.image(img, img.shape, frameno, extra_fields=extra)
            logger.debug("sent frame %d", frameno)
            self.stat.packets += 1
            await asyncio.sleep(spec.time)
        await acq.close()
        self.stat.packets += 1

    async def close_socket(self) -> None:
        if self.in_socket is not None:
            logger.info("closing in_socket %s", self.in_socket)
            self.in_socket.close()
        if self.out_socket is not None:
            logger.info("closing out_socket %s", self.out_socket.data_socket)
            await self.out_socket.close()
        logger.info("sockets closed")

    async def close(self) -> None:
        self.context.destroy(linger=0)
        logger.info("context destroyed")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    gen = WorkloadGenerator()
    app.state.gen = gen
    task = asyncio.create_task(gen.calc_stat())
    yield
    await cancel_and_wait(task)
    await gen.close()
    # Clean up the ML models and release the resources


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)

    @app.get("/api/v1/finished")
    async def get_fin(request: Request) -> bool:
        return request.app.state.gen.finished()

    @app.get("/api/v1/config")
    async def get_conf() -> NetworkConfig:
        return NetworkConfig(
            addresses=psutil.net_if_addrs(), stats=psutil.net_if_stats()
        )

    @app.get("/api/v1/statistics")
    async def get_stat(request: Request) -> Statistics:
        return request.app.state.gen.stat

    @app.post("/api/v1/open_socket")
    async def open_sock(request: Request, sockspec: SocketSpec) -> SocketSpec:
        print(sockspec)
        await request.app.state.gen.open_socket(sockspec)
        return sockspec

    @app.post("/api/v1/frames")
    async def frames(request: Request, spec: WorkloadSpec) -> bool:
        gen = request.app.state.gen
        gen.task = asyncio.create_task(gen.send_packets(spec))
        gen.task.add_done_callback(done_callback)
        return True

    @app.post("/api/v1/close_socket")
    async def close_sock(request: Request) -> bool:
        await request.app.state.gen.close_socket()
        return True

    @app.post("/api/v1/connect_socket")
    async def connect_sock(request: Request, sockspec: ConnectSpec) -> ConnectSpec:
        await request.app.state.gen.connect_socket(sockspec)
        return sockspec

    return app


app = create_app()
