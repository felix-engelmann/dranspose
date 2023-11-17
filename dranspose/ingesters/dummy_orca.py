import numpy as np
import zmq
from pydantic_core import Url

from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import IngesterName


class DummyOrcaIngester(Ingester):
    def __init__(self):
        super().__init__(IngesterName("dummy_orca"), IngesterSettings(worker_url=Url("tcp://localhost:10006")))
        self.state.streams = ["orca"]

    async def run_source(self, stream):
        img = np.zeros((8, 8), dtype=np.uint16)
        # print("generated image")
        parts = [b"header for orca", zmq.Frame(img.tobytes())]
        while True:
            yield parts
