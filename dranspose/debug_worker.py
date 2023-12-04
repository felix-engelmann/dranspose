import asyncio
import logging
import pickle
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any

from fastapi import FastAPI
from starlette.responses import Response

from dranspose.event import EventData
from dranspose.protocol import WorkerUpdate, WorkerStateEnum, RedisKeys
from dranspose.worker import Worker, RedisException

logger = logging.getLogger(__name__)


class DebugWorker(Worker):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.buffer: deque[EventData] = deque(maxlen=50)

    async def work(self) -> None:
        self._logger.info("started work task")
        await self.notify_worker_ready()

        lastev = "0"
        while True:
            try:
                newlastev, ingesterset = await self.get_new_assignments(lastev)
                if newlastev is None or ingesterset is None:
                    continue
                lastev = newlastev
            except RedisException:
                self._logger.warning("failed to access redis, exiting worker")
                break

            if len(ingesterset) == 0:
                continue
            done = await self.collect_internals(ingesterset)

            event = await self.build_event(done)
            self._logger.debug("adding event %s to buffer", event)
            self.buffer.append(event)

            wu = WorkerUpdate(
                state=WorkerStateEnum.IDLE,
                completed=event.event_number,
                worker=self.state.name,
            )
            await self.redis.xadd(
                RedisKeys.ready(self.state.mapping_uuid),
                {"data": wu.model_dump_json()},
            )


worker: DebugWorker


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    global worker
    worker = DebugWorker()
    run_task = asyncio.create_task(worker.run())
    yield
    run_task.cancel()
    await worker.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.get("/api/v1/status")
async def get_status() -> bool:
    return True


@app.get("/api/v1/last_event")
async def get_result() -> Response:
    data = b""
    try:
        lev = worker.buffer[-1]
        byteev = EventData(
            event_number=lev.event_number,
            streams={k: v.get_bytes() for k, v in lev.streams.items()},
        )
        data = pickle.dumps(byteev)
    except Exception as e:
        logging.warning("no publishable data: %s", e.__repr__())
    return Response(data, media_type="application/x.pickle")
