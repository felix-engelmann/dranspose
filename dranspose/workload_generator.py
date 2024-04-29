import asyncio
import logging
import random
import time
from asyncio import Task
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any, Tuple

import numpy as np
import psutil
import zmq.asyncio
from fastapi import FastAPI
from psutil._common import snicaddr, snetio, snicstats
from pydantic import BaseModel, Field, field_serializer
from pydantic_core import Url

from dranspose.helpers.utils import cancel_and_wait
from tests.stream1 import AcquisitionSocket

logger = logging.getLogger(__name__)


class SocketSpec(BaseModel):
    type: int = zmq.PUSH
    port: int = 9999


class WorkloadSpec(BaseModel):
    number: int = 1
    time: float = 0.1
    shape: Tuple[int, int] = (10, 10)


class Statistics(BaseModel):
    snapshots: list[tuple[float, int, dict[str, snetio]]] = Field(
        default_factory=lambda: deque(maxlen=100)
    )
    fps: float = 0
    sent: int = 0
    measured: float = 0

    @field_serializer("snapshots", mode="wrap")
    def serialize_sn(self, snapshots, _info):
        return list(snapshots)


class NetworkConfig(BaseModel):
    addresses: dict[str, list[snicaddr]]
    stats: dict[str, snicstats]


class WorkloadGenerator:
    def __init__(self, **kwargs: Any) -> None:
        self.context = zmq.asyncio.Context()
        self.socket: AcquisitionSocket | None = None
        self.task: Task[Any] | None = None
        self.stat = Statistics()
        logger.info("Workload generator initialised")

    def finished(self) -> bool:
        if self.task is None:
            return True
        return self.task.done()

    async def calc_stat(self) -> None:
        start = time.time()
        before_sent = self.stat.sent
        while True:
            await asyncio.sleep(0.5)
            after = time.time()
            num = self.stat.sent
            ctr = psutil.net_io_counters(pernic=True)
            self.stat.measured = after
            self.stat.fps = (num - before_sent) / (after - start)
            self.stat.snapshots.append((after, num, ctr))
            logger.debug("calculated stats %s fps", self.stat.fps)
            start = after
            before_sent = num

    async def open_socket(self, spec: SocketSpec) -> None:
        await self.close_socket()
        self.fps = 0
        self.sent = 0
        self.socket = AcquisitionSocket(
            self.context, Url(f"tcp://*:{spec.port}"), typ=spec.type
        )
        logger.info("socket opened to %s", spec)

    async def packets(self, spec: WorkloadSpec) -> None:
        if self.socket is None:
            raise Exception("must open socket before sending packets")
        acq = await self.socket.start(filename="")
        logger.info("sending packets %s", spec)
        self.stat.sent += 1
        width = spec.shape[0]
        height = spec.shape[1]
        img = np.zeros((width, height), dtype=np.uint16)
        for _ in range(20):
            img[random.randint(0, width - 1)][
                random.randint(0, height - 1)
            ] = random.randint(0, 10)
        for frameno in range(spec.number):
            await acq.image(img, img.shape, frameno)
            logger.debug("sent frame %d", frameno)
            self.stat.sent += 1
            await asyncio.sleep(spec.time)
        await acq.close()
        self.stat.sent += 1

    async def close_socket(self) -> None:
        if self.socket is not None:
            await self.socket.close()
        logger.info("socket closed")

    async def close(self) -> None:
        self.context.destroy(linger=0)
        logger.info("context destroyed")


gen: WorkloadGenerator


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    global gen
    gen = WorkloadGenerator()
    task = asyncio.create_task(gen.calc_stat())
    yield
    await cancel_and_wait(task)
    await gen.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.get("/api/v1/finished")
async def get_fin() -> bool:
    global gen
    return gen.finished()


@app.get("/api/v1/config")
async def get_conf() -> NetworkConfig:
    return NetworkConfig(addresses=psutil.net_if_addrs(), stats=psutil.net_if_stats())


@app.get("/api/v1/statistics")
async def get_stat() -> Statistics:
    global gen
    return gen.stat


@app.post("/api/v1/open_socket")
async def sock(sockspec: SocketSpec) -> SocketSpec:
    global gen
    print(sockspec)
    await gen.open_socket(sockspec)
    return sockspec


@app.post("/api/v1/frames")
async def frames(spec: WorkloadSpec) -> bool:
    global gen
    gen.task = asyncio.create_task(gen.packets(spec))
    return True


@app.post("/api/v1/close_socket")
async def close_sock() -> bool:
    global gen
    await gen.close_socket()
    return True
