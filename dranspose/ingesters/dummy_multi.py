from typing import AsyncGenerator

import numpy as np
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import IngesterName, StreamName


class DummyMultiIngester(Ingester):
    def __init__(self) -> None:
        super().__init__(
            IngesterName("dummy_alba"),
            IngesterSettings(worker_url=Url("tcp://localhost:10007")),
        )
        self.state.streams = [StreamName("alba"), StreamName("slow")]

    async def run_source(
        self, stream: StreamName
    ) -> AsyncGenerator[list[bytes | zmq.Frame], None]:
        parts: list[bytes | zmq.Frame]
        if stream == "alba":
            val = np.array([2.4])
            parts = [b"header for alba", val.tobytes()]
        elif stream == "slow":
            parts = [b"slow header", b"const"]
        else:
            raise Exception("stream not handled by this ingester")
        while True:
            yield parts
