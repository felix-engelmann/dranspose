import asyncio
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator

import zmq.asyncio
from fastapi import FastAPI
from pydantic import UUID4

from dranspose.distributed import DistributedService, DistributedSettings
from dranspose.protocol import ReducerState, ZmqUrl, RedisKeys


class ReducerSettings(DistributedSettings):
    reducer_url: ZmqUrl = ZmqUrl("tcp://localhost:10000")

class Reducer(DistributedService):
    def __init__(self, settings: Optional[ReducerSettings] = None):
        self._reducer_settings = settings
        if self._reducer_settings is None:
            self._reducer_settings = ReducerSettings()

        state = ReducerState(url=self._reducer_settings.reducer_url)
        super().__init__(state, self._reducer_settings)
        self.state: ReducerState
        self.ctx = zmq.asyncio.Context()

    async def run(self) -> None:
        self.work_task = asyncio.create_task(self.work())
        await self.register()

    async def work(self) -> None:
        pass

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