import numpy as np
import zmq

from dranspose.ingester import Ingester


class DummyEigerIngester(Ingester):
    def __init__(self):
        super().__init__("dummy_ingester", config={"worker_port": 10005})
        self.state.streams = ["eiger"]

    async def get_frame(self, stream):
        img = np.zeros((4, 4), dtype=np.uint16)
        # print("generated image")
        parts = [b"header for eiger", zmq.Frame(img.tobytes())]
        return parts
