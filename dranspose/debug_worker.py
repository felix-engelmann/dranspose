import asyncio
import logging
import pickle
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any

from fastapi import FastAPI
from starlette.responses import Response

from dranspose.event import EventData
from dranspose.helpers.utils import done_callback, cancel_and_wait
from dranspose.protocol import WorkerUpdate, DistributedStateEnum, RedisKeys
from dranspose.worker import Worker

logger = logging.getLogger(__name__)


class DebugWorker(Worker):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.buffer: deque[EventData] = deque(maxlen=50)

    async def work(self) -> None:
        self._logger.info("started work task")
        await self.notify_worker_ready()

        while True:
            self.dequeue_task = None
            self.dequeue_task = asyncio.create_task(self.assignment_queue.get())
            evn, streamset = await self.dequeue_task

            self.new_data = asyncio.Future()
            while set(self.stream_queues.get(evn, {}).keys()) != streamset:
                await self.new_data
                self.new_data = asyncio.Future()
            self.new_data = None

            event = EventData(event_number=evn, streams=self.stream_queues[evn])
            self._logger.debug("adding event %s to buffer", event)
            self.buffer.append(event)

            wu = WorkerUpdate(
                state=DistributedStateEnum.IDLE,
                completed=[event.event_number],
                has_result=[False],
                worker=self.state.name,
            )
            await self.redis.xadd(
                RedisKeys.ready(self.state.mapping_uuid),
                {"data": wu.model_dump_json()},
            )
            self.state.processed_events += 1


worker: DebugWorker


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    global worker
    worker = DebugWorker()
    run_task = asyncio.create_task(worker.run())
    run_task.add_done_callback(done_callback)
    yield
    await cancel_and_wait(run_task)
    await worker.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.get("/api/v1/status")
async def get_status() -> bool:
    return True


@app.get("/api/v1/last_events")
async def get_result(number: int = 1) -> Response:
    global worker
    print("getting ", number, "events")
    byteevs = []
    for i in range(-1, -number - 1, -1):
        try:
            lev = worker.buffer[i]
            byteevs.append(
                EventData(
                    event_number=lev.event_number,
                    streams={k: v.get_bytes() for k, v in lev.streams.items()},
                )
            )
        except Exception:
            pass
    data = pickle.dumps(list(reversed(byteevs)))
    return Response(data, media_type="application/x.pickle")
