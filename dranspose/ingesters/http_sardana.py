import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, Any

import zmq
from fastapi import FastAPI, Body

from dranspose.data.sardana import (
    SardanaDataDescription,
    SardanaRecordEnd,
    SardanaRecordData,
    SardanaPacketType,
)
from dranspose.event import StreamData
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import StreamName


logger = logging.getLogger(__name__)


class HttpSardanaSettings(IngesterSettings):
    pass


class HttpSardanaIngester(Ingester):
    def __init__(self, settings: Optional[HttpSardanaSettings] = None) -> None:
        if settings is not None:
            self._http_sardana_settings = settings
        else:
            self._http_sardana_settings = HttpSardanaSettings()

        super().__init__(settings=self._http_sardana_settings)
        self.in_queue: Optional[asyncio.Queue[SardanaPacketType]] = asyncio.Queue()

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        self.in_queue = asyncio.Queue()
        while True:
            self._logger.debug("clear up insocket")
            packet = await self.in_queue.get()
            if isinstance(packet, SardanaDataDescription):
                self._logger.info("start of new sequence %s", packet)
                frame = zmq.Frame(packet.model_dump_json().encode("utf8"))
                yield StreamData(typ="sardana", frames=[frame])
                break
        while True:
            if self.in_queue is not None:
                packet = await self.in_queue.get()
            else:
                break
            frame = zmq.Frame(packet.model_dump_json().encode("utf8"))
            self._logger.debug("send stream data %s", frame.bytes)
            yield StreamData(typ="sardana", frames=[frame])
            if isinstance(packet, SardanaRecordEnd):
                break
        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_queue.get()

    async def stop_source(self, stream: StreamName) -> None:
        if self.in_queue:
            self._logger.info("closing queue")
            self.in_queue = None

    async def receive(self, paket: SardanaPacketType) -> None:
        if self.in_queue is not None:
            await self.in_queue.put(paket)


sardana_ingester: HttpSardanaIngester


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    global sardana_ingester
    sardana_ingester = HttpSardanaIngester()
    run_task = asyncio.create_task(sardana_ingester.run())
    yield
    run_task.cancel()
    await sardana_ingester.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)


@app.post("/v1/data_desc")
async def post_data_desc(payload: dict[str, Any] = Body(...)) -> None:
    global sardana_ingester
    await sardana_ingester.receive(SardanaDataDescription(description=payload))
    logging.info("data_desc received %s", payload)


@app.post("/v1/record_data")
async def post_record_data(payload: dict[str, Any] = Body(...)) -> None:
    global sardana_ingester
    await sardana_ingester.receive(SardanaRecordData(record=payload))
    logging.debug("record_data received %s", payload)


@app.post("/v1/custom_data")
async def post_custom_data(payload: dict[str, Any] = Body(...)) -> None:
    global sardana_ingester
    logging.debug("ignoring custom_data received %s", payload)


@app.post("/v1/record_end")
async def post_record_end(payload: dict[str, Any] = Body(...)) -> None:
    global sardana_ingester
    await sardana_ingester.receive(SardanaRecordEnd(end=payload))
    logging.debug("record_end received %s", payload)
