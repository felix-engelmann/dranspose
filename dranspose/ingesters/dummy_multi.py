import numpy as np
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import IngesterName


class DummyMultiIngester(Ingester):
    def __init__(self):
        super().__init__(IngesterName("dummy_alba"), IngesterSettings(worker_url=Url("tcp://localhost:10007")))
        self.state.streams = ["alba", "slow"]

    async def run_source(self, stream):
        if stream == "alba":
            val = np.array([2.4])
            parts = [b"header for alba", val.tobytes()]
        elif stream == "slow":
            parts = [b"slow header", b"const"]
        else:
            raise Exception("stream not handled by this ingester")
        while True:
            yield parts
