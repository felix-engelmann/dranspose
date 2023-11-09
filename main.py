import asyncio
import logging

from dranspose.controller import Controller
from dranspose.ingester import Ingester
from dranspose.worker import Worker

logging.basicConfig(level=logging.DEBUG)

async def main():
    ctrl = Controller()
    i1 = Ingester("tcp://127.0.0.1:9999", b'ingest1')
    w1 = Worker("tcp://127.0.0.1:9999", b'worker1')
    w2 = Worker("tcp://127.0.0.1:9999", b'worker2')
    asyncio.create_task(i1.run())
    asyncio.create_task(w1.run())
    asyncio.create_task(w2.run())
    await ctrl.run()
    print("done init")

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("exiting")