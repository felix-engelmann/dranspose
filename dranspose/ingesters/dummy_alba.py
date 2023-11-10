import numpy as np
import zmq

from dranspose.ingester import Ingester


class DummyAlbaIngester(Ingester):
    def __init__(self):
        super().__init__("dummy_alba", config={"worker_port": 10007})
        self.state.streams = ["alba"]

    async def get_frame(self, stream):
        val = np.array([2.4])
        parts = [b"header for alba", val.tobytes()]
        return parts
