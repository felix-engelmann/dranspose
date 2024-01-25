import asyncio
import logging
import pickle
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any

import zmq
from fastapi import FastAPI
from starlette.responses import Response

from dranspose.event import EventData, ResultData
from dranspose.helpers.utils import done_callback
from dranspose.protocol import WorkerUpdate, DistributedStateEnum, RedisKeys
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
            done = await self.poll_internals(ingesterset)
            if set(done) != {zmq.POLLIN}:
                self._logger.warning("not all sockets are pollIN %s", done)
                continue

            event = await self.build_event(ingesterset)
            self._logger.debug("adding event %s to buffer", event)
            self.buffer.append(event)

            # send none result to reducer as reducer expects it
            rd = ResultData(
                event_number=event.event_number,
                worker=self.state.name,
                payload=None,
                parameters_hash=self.state.parameters_hash,
            )
            if self.out_socket:
                try:
                    header = rd.model_dump_json(exclude={"payload"}).encode("utf8")
                    body = pickle.dumps(rd.payload)
                    self._logger.debug(
                        "send result to reducer with header %s, len-payload %d",
                        header,
                        len(body),
                    )
                    await self.out_socket.send_multipart([header, body])
                except Exception as e:
                    self._logger.error("could not dump result %s", e.__repr__())

            wu = WorkerUpdate(
                state=DistributedStateEnum.IDLE,
                completed=event.event_number,
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
    run_task.cancel()
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
