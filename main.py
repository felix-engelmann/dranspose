import asyncio
import logging
import uvicorn
from dranspose.controller import app
from dranspose.ingester import Ingester
from dranspose.worker import Worker

logging.basicConfig(level=logging.DEBUG)

async def main():
    i1 = Ingester(b'ingest1')
    w1 = Worker(b'worker1')
    w2 = Worker(b'worker2')
    asyncio.create_task(i1.run())
    asyncio.create_task(w1.run())
    asyncio.create_task(w2.run())

    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

    await w1.close()
    await w2.close()
    await i1.close()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("exiting")