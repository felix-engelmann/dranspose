import asyncio
import importlib
import json
import logging
import os
import pickle
import sys
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator, Any

import zmq.asyncio
from fastapi import FastAPI
from pydantic import UUID4
from starlette.responses import Response

from dranspose.distributed import DistributedService, DistributedSettings
from dranspose.event import ResultData
from dranspose.protocol import ReducerState, ZmqUrl, RedisKeys

logger = logging.getLogger(__name__)


class ReducerSettings(DistributedSettings):
    reducer_url: ZmqUrl = ZmqUrl("tcp://localhost:10200")
    reducer_class: Optional[str] = None


class Reducer(DistributedService):
    def __init__(self, settings: Optional[ReducerSettings] = None):
        self._reducer_settings = settings
        if self._reducer_settings is None:
            self._reducer_settings = ReducerSettings()

        state = ReducerState(url=self._reducer_settings.reducer_url)
        super().__init__(state, self._reducer_settings)
        self.state: ReducerState
        self.ctx = zmq.asyncio.Context()
        self.in_socket = self.ctx.socket(zmq.PULL)
        self.in_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.in_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.in_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 300)
        self.in_socket.bind(f"tcp://*:{self._reducer_settings.reducer_url.port}")

        self.custom = None
        if self._reducer_settings.reducer_class:
            try:
                sys.path.append(os.getcwd())
                module = importlib.import_module(
                    self._reducer_settings.reducer_class.split(":")[0]
                )
                self._logger.info("loaded module %s", module)
                self.custom = getattr(
                    module, self._reducer_settings.reducer_class.split(":")[1]
                )
                self._logger.info("custom reducer class %s", self.custom)
            except:
                self._logger.warning(
                    "no custom reducer class loaded, discarding results"
                )

    async def run(self) -> None:
        self.work_task = asyncio.create_task(self.work())
        await self.register()

    async def work(self) -> None:
        self._logger.info("started work task")
        self.reducer = None
        if self.custom:
            self.reducer = self.custom()
        while True:
            parts = await self.in_socket.recv_multipart()
            prelim = json.loads(parts[0])
            prelim["payload"] = pickle.loads(parts[1])
            result = ResultData.model_validate(prelim)
            if self.reducer:
                try:
                    result = self.reducer.process_result(result, self.parameters)
                except Exception as e:
                    self._logger.error("custom reducer failed: %s", e.__repr__())
            self._logger.debug("received %s", result)

    async def restart_work(self, new_uuid: UUID4) -> None:
        self._logger.info("resetting config %s", new_uuid)
        self.work_task.cancel()
        self.state.mapping_uuid = new_uuid
        self.work_task = asyncio.create_task(self.work())

    async def close(self) -> None:
        self.work_task.cancel()
        await self.redis.delete(RedisKeys.config("reducer", self.state.name))
        await self.redis.aclose()
        self.ctx.destroy(linger=0)
        self._logger.info("closed reducer")


reducer: Reducer


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    global reducer
    reducer = Reducer()
    run_task = asyncio.create_task(reducer.run())
    yield
    run_task.cancel()
    await reducer.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.get("/api/v1/status")
async def get_status() -> bool:
    return True


@app.get("/api/v1/result/pickle")
async def get_result() -> Any | bytes:
    data = b""
    try:
        if hasattr(reducer.reducer, "publish"):
            data = pickle.dumps(reducer.reducer.publish)
    except:
        logging.warning("no publishable data")
    return Response(data, media_type="application/x.pickle")
