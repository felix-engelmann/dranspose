import asyncio
import numpy as np
import zmq

from dranspose.ingester import Ingester

class DummyEigerIngester(Ingester):
    def __init__(self):
        super().__init__("dummy_ingster", config={"worker_port":10005})
        self.state.streams = ["dummyeiger"]
        loop = asyncio.get_event_loop()
        self.started = loop.create_future()

    def start_frames(self):
        self.started.set_result(True)

    async def work(self):
        await self.started
        frameno = 0
        while True:
            img = np.zeros((100,100), dtype=np.uint16)
            print("generated image")
            parts = [zmq.Frame(img.tobytes())]
            await self.handle_frame(frameno, parts)
            frameno += 1
            await asyncio.sleep(1)
