from typing import AsyncGenerator

import numpy as np
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import IngesterName, StreamName


class DummyOrcaIngester(Ingester):
    def __init__(self) -> None:
        super().__init__(IngesterName("dummy_orca"), IngesterSettings(worker_url=Url("tcp://localhost:10006")))
        self.state.streams = [StreamName("orca")]

    async def run_source(self, stream: StreamName) -> AsyncGenerator[list[bytes | zmq.Frame], None]:
        img = np.zeros((8, 8), dtype=np.uint16)
        # print("generated image")
        parts: list[bytes|zmq.Frame] = [b"header for orca", zmq.Frame(img.tobytes())]
        while True:
            yield parts
