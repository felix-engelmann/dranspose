import asyncio
import logging
import uvicorn
from dranspose.controller import app
from dranspose.ingester import Ingester
from dranspose.worker import Worker

logging.basicConfig(level=logging.DEBUG)

async def main():
    iconfs = [{"worker_port":10000, "streams":["orca"]}, {"worker_port":10001, "streams":["alba","eiger"]}]
    ins = [Ingester('ingest'+str(i+1), config=config) for i,config in enumerate(iconfs)]
    wos = [Worker('worker'+str(i)) for i in range(1, 3)]

    for i in ins+wos:
        asyncio.create_task(i.run())

    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

    for i in ins + wos:
        await i.close()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("exiting")