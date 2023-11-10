import asyncio
import logging
import threading

import uvicorn
import requests
from dranspose.controller import app
from dranspose.ingester import Ingester
from dranspose.ingesters.dummy_multi import DummyMultiIngester
from dranspose.ingesters.dummy_eiger import DummyEigerIngester
from dranspose.ingesters.dummy_orca import DummyOrcaIngester
from dranspose.worker import Worker

logging.basicConfig(level=logging.DEBUG)

async def main():
    ins = []
    ins.append(DummyEigerIngester())
    ins.append(DummyOrcaIngester())
    ins.append(DummyMultiIngester())
    wos = [Worker('worker'+str(i)) for i in range(1, 3)]

    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    await asyncio.sleep(5)

    def req():
        return requests.post("http://localhost:5000/mapping")

    threading.Thread(target=req).start()

    await server_task

    for i in ins + wos:
        await i.close()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("exiting")