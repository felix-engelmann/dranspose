import argparse
import asyncio
import logging
import threading
from multiprocessing import Process

import uvicorn
import requests
from dranspose.controller import app
from dranspose.ingesters.dummy_multi import DummyMultiIngester
from dranspose.ingesters.dummy_eiger import DummyEigerIngester
from dranspose.ingesters.dummy_orca import DummyOrcaIngester
from dranspose.ingesters.streaming_single import StreamingSingleIngester
from dranspose.worker import Worker

logging.basicConfig(level=logging.DEBUG)


async def main():
    ins = []
    # ins.append(DummyEigerIngester())
    # ins.append(DummyOrcaIngester())
    ins.append(DummyMultiIngester())
    ins.append(
        StreamingSingleIngester(connect_url="tcp://localhost:9999", name="eiger")
    )
    wos = [Worker("worker" + str(i)) for i in range(1, 3)]

    for i in ins + wos:
        asyncio.create_task(i.run())

    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    await asyncio.sleep(5)

    def req():
        ret = requests.post(
            "http://localhost:5000/api/v1/mapping",
            json={
                "eiger": [[3], [5], [7], [9], [11], [13], [15], [17], [19]],
                "slow": [None, None, [1006], None, None, [1012], None, None, [1018]],
            },
        )
        print("requests returned", ret.content)

    threading.Thread(target=req).start()

    await server_task

    for i in ins + wos:
        await i.close()
