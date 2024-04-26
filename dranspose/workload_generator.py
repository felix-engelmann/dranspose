import asyncio
import logging
from asyncio import Task
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any

import numpy as np
import zmq.asyncio
from fastapi import FastAPI
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SocketSpec(BaseModel):
    type: int = zmq.PUSH
    port: int = 9999


class WorkloadGenerator:
    def __init__(self, **kwargs: Any) -> None:
        self.context = zmq.asyncio.Context()
        self.socket: zmq.Socket[Any] | None = None
        self.task: Task[Any] | None = None

    def status(self):
        if self.task is None:
            return True
        return self.task.done()

    def open_socket(self, spec: SocketSpec) -> None:
        self.close_socket()
        self.socket = self.context.socket(spec.type)
        self.socket.bind(f"tcp://*:{spec.port}")

    async def packets(self, num, time):
        for i in range(num):
            logger.debug("send packet %d", i)
            data = np.zeros((10, 10))
            await self.socket.send(data)
            await asyncio.sleep(time)

    def empty_packets(self, num, time):
        logger.debug("create task")
        self.task = asyncio.create_task(self.packets(num, time))

    def close_socket(self) -> None:
        if self.socket is not None:
            self.socket.close()

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
    gen.open_socket(sockspec)
    return sockspec


@app.post("/api/v1/frames")
async def frames(number: int = 5, time: float = 0.2) -> bool:
    global gen
    logger.debug("start of packets")
    gen.empty_packets(number, time)
    return True


@app.post("/api/v1/close_socket")
async def close_sock() -> bool:
    global gen
    gen.close_socket()
    return True
