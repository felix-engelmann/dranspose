import numpy as np
import zmq

from dranspose.ingester import Ingester


class DummyMultiIngester(Ingester):
    def __init__(self):
        super().__init__("dummy_alba", config={"worker_port": 10007})
        self.state.streams = ["alba","slow"]

    async def get_frame(self, stream):
        if stream == "alba":
            val = np.array([2.4])
            parts = [b"header for alba", val.tobytes()]
        elif stream == "slow":
            parts =[b"slow header", b"const"]
        else:
            raise Exception("stream not handled by this ingester")
        return parts
