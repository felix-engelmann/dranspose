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
    #ins.append(DummyEigerIngester())
    #ins.append(DummyOrcaIngester())
    #ins.append(DummyMultiIngester())
    ins.append(StreamingSingleIngester(connect_url="tcp://localhost:9999", name="eiger"))
    wos = [Worker('worker'+str(i)) for i in range(1, 3)]

    for i in ins + wos:
        asyncio.create_task(i.run())

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='dranspose',
        description='Transposes Streams')

    parser.add_argument('component', choices=["controller","worker","ingester","combined"])  # positional argument
    parser.add_argument('-n', '--name')  # option that takes a value
    parser.add_argument('-c', '--ingestclass')  # option that takes a value

    args = parser.parse_args()
    print(args)
    if args.component == "controller":
        try:
            config = uvicorn.Config(app, port=5000, log_level="info")
            server = uvicorn.Server(config)
            server.run()
        except KeyboardInterrupt:
            print("exiting")
    elif args.component == "ingester":
        print(args.ingestclass)
        ing = globals()[args.ingestclass]
        async def run():
            i = ing()
            await i.run()
            await i.close()

        asyncio.run(run())
    elif args.component == "worker":
        print(args.name)
        async def run():
            w = Worker(args.name)
            await w.run()
            await w.close()

        asyncio.run(run())
    elif args.component == "combined":
        asyncio.run(main())