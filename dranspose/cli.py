import argparse
import asyncio
import os

import uvicorn

from dranspose.controller import app
from dranspose.ingesters.streaming_single import StreamingSingleIngester
from dranspose.worker import Worker

async def main():
    ins = []
    ins.append(
        StreamingSingleIngester(connect_url="tcp://localhost:9999", name="eiger")
    )
    wos = [Worker("worker" + str(i)) for i in range(1, 3)]

    for i in ins + wos:
        asyncio.create_task(i.run())

    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await server_task

    for i in ins + wos:
        await i.close()

def run():
    parser = argparse.ArgumentParser(prog="dranspose", description="Transposes Streams")

    parser.add_argument(
        "component", choices=["controller", "worker", "ingester", "combined"]
    )  # positional argument
    parser.add_argument("-n", "--name")  # option that takes a value
    parser.add_argument("-c", "--ingestclass")  # option that takes a value

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
            i = ing(redis_host=os.getenv("REDIS_HOST","localhost"),
                    redis_port=os.getenv("REDIS_PORT",6379),
                    worker_url=os.getenv("WORKER_URL", None))
            await i.run()
            await i.close()

        asyncio.run(run())
    elif args.component == "worker":
        print(args.name)

        async def run():
            w = Worker(args.name,
                       redis_host=os.getenv("REDIS_HOST","localhost"),
                       redis_port=os.getenv("REDIS_PORT",6379))
            await w.run()
            await w.close()

        asyncio.run(run())
    elif args.component == "combined":
        asyncio.run(main())

if __name__ == "__main__":
    run()