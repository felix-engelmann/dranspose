import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any

import zmq
from fastapi import FastAPI
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SocketSpec(BaseModel):
    type: int = zmq.PUSH
    port: int = 9999


class WorkloadGenerator:
    def __init__(self, **kwargs: Any) -> None:
        self.context = zmq.Context()
        self.socket: zmq.Socket[Any] | None = None

    def open_socket(self, spec: SocketSpec) -> None:
        self.close_socket()
        self.socket = self.context.socket(spec.type)
        self.socket.bind(f"tcp://*:{spec.port}")

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
    return True


@app.post("/api/v1/open_socket")
async def sock(sockspec: SocketSpec) -> SocketSpec:
    global gen
    print(sockspec)
    gen.open_socket(sockspec)
    return sockspec


@app.post("/api/v1/close_socket")
async def close_sock() -> bool:
    global gen
    gen.close_socket()
    return True
