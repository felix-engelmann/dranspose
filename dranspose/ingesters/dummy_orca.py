import numpy as np
import zmq

from dranspose.ingester import Ingester


class DummyOrcaIngester(Ingester):
    def __init__(self):
        super().__init__("dummy_orca", config={"worker_port": 10006})
        self.state.streams = ["orca"]

    async def get_frame(self, stream):
        img = np.zeros((8, 8), dtype=np.uint16)
        #print("generated image")
        parts = [b"header for orca", zmq.Frame(img.tobytes())]
        return parts
