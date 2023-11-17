import asyncio
from typing import List, AsyncGenerator

import numpy as np
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import IngesterName, StreamName


class DummyEigerIngester(Ingester):
    def __init__(self) -> None:
        super().__init__(IngesterName("dummy_ingester"), IngesterSettings(worker_url=Url("tcp://localhost:10005")))
        self.state.streams = [StreamName("eigerdummy")]

    async def run_source(self, stream: StreamName) -> AsyncGenerator[list[bytes | zmq.Frame], None]:
        img = np.zeros((1000, 1000), dtype=np.uint16)
        # print("generated image")
        parts: list[bytes|zmq.Frame] = [b"header for eiger", zmq.Frame(img.tobytes())]
        while True:
            yield parts
            # await asyncio.sleep(0.1)
