import asyncio
import logging
import random
from asyncio import Task
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any, Tuple

import numpy as np
import zmq.asyncio
from fastapi import FastAPI
from pydantic import BaseModel
from pydantic_core import Url

from tests.stream1 import AcquisitionSocket

logger = logging.getLogger(__name__)


class SocketSpec(BaseModel):
    type: int = zmq.PUSH
    port: int = 9999


class WorkloadGenerator:
    def __init__(self, **kwargs: Any) -> None:
        self.context = zmq.asyncio.Context()
        self.socket: AcquisitionSocket | None = None
        self.task: Task[Any] | None = None

    def status(self) -> bool:
        if self.task is None:
            return True
        return self.task.done()

    async def open_socket(self, spec: SocketSpec) -> None:
        await self.close_socket()
        self.socket = AcquisitionSocket(
            self.context, Url(f"tcp://*:{spec.port}"), typ=spec.type
        )

    async def packets(self, num: int, time: float, shape: Tuple[int, int]) -> None:
        if self.socket is None:
            raise Exception("must open socket before sending packets")
        acq = await self.socket.start(filename="")
        width = shape[0]
        height = shape[1]
        img = np.zeros((width, height), dtype=np.uint16)
        for _ in range(20):
            img[random.randint(0, width - 1)][
                random.randint(0, height - 1)
            ] = random.randint(0, 10)
        for frameno in range(num):
            await acq.image(img, img.shape, frameno)
            await asyncio.sleep(time)
        await acq.close()

    async def close_socket(self) -> None:
        if self.socket is not None:
            await self.socket.close()

    async def close(self) -> None:
        self.context.destroy(linger=0)


gen: WorkloadGenerator


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    global gen
    gen = WorkloadGenerator()
    yield
    await gen.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.get("/api/v1/status")
async def get_status() -> bool:
    global gen
    return gen.status()


@app.post("/api/v1/open_socket")
async def sock(sockspec: SocketSpec) -> SocketSpec:
    global gen
    print(sockspec)
    await gen.open_socket(sockspec)
    return sockspec


@app.post("/api/v1/frames")
async def frames(
    number: int = 5, time: float = 0.2, shape: Tuple[int, int] = (10, 10)
) -> bool:
    global gen
    logger.debug("start of packets")
    gen.task = asyncio.create_task(gen.packets(number, time, shape))
    return True


@app.post("/api/v1/close_socket")
async def close_sock() -> bool:
    global gen
    await gen.close_socket()
    return True
