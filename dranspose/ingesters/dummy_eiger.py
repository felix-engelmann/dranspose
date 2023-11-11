import asyncio
from typing import List

import numpy as np
import zmq

from dranspose.ingester import Ingester


class DummyEigerIngester(Ingester):
    def __init__(self):
        super().__init__("dummy_ingester", config={"worker_port": 10005})
        self.state.streams = ["eigerdummy"]

    async def run_source(self, stream):
        img = np.zeros((1000, 1000), dtype=np.uint16)
        # print("generated image")
        parts = [b"header for eiger", zmq.Frame(img.tobytes())]
        while True:
            yield parts
            #await asyncio.sleep(0.1)
