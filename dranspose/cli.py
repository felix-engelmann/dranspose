import argparse
import asyncio
import logging
import os
import random
import socket
import string
from typing import Literal

import uvicorn
from pydantic_core import Url
from pydantic_settings import BaseSettings

from dranspose.controller import app
from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
from dranspose.protocol import StreamName, WorkerName
from dranspose.worker import Worker

class CliSettings(BaseSettings):
    log_level: Literal["DEBUG","INFO","WARNING","ERROR"] = "INFO"

settings = CliSettings()

logging.basicConfig(level=settings.log_level.upper())

async def main() -> None:
    ins = []
    ins.append(
        StreamingSingleIngester(
            name=StreamName("eiger"),
            settings=StreamingSingleSettings(upstream_url=Url("tcp://localhost:9999")),
        )
    )
    wos = [Worker(WorkerName("worker" + str(i))) for i in range(1, 3)]

    for i in ins + wos:
        asyncio.create_task(i.run())

    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await server_task

    for i in ins + wos:
        await i.close()


def run() -> None:
    parser = argparse.ArgumentParser(prog="dranspose", description="Transposes Streams")

    parser.add_argument(
        "component", choices=["controller", "worker", "ingester", "combined"]
    )  # positional argument
    parser.add_argument("-n", "--name")  # option that takes a value
    parser.add_argument("-c", "--ingestclass")  # option that takes a value
    parser.add_argument("-u", "--connect_url")  # option that takes a value
    parser.add_argument("-h", "--host")  # option that takes a value

    args = parser.parse_args()
    print(args)
    if args.component == "controller":
        try:
            config = uvicorn.Config(app, port=5000, host=args.host , log_level="info")
            server = uvicorn.Server(config)
            server.run()
        except KeyboardInterrupt:
            print("exiting")
    elif args.component == "ingester":
        print(args.ingestclass)
        ing = globals()[args.ingestclass]

        async def run() -> None:
            i = ing(
                name=args.name,
                settings=StreamingSingleSettings(upstream_url=args.connect_url),
            )
            await i.run()
            await i.close()

        asyncio.run(run())
    elif args.component == "worker":
        name = args.name
        if not name:
            randid = "".join([random.choice(string.ascii_letters) for _ in range(10)])
            name = "Worker-{}-{}".format(socket.gethostname(), randid).encode("ascii")
        print("worker name:", name)
        async def run() -> None:
            w = Worker(name)
            await w.run()
            await w.close()

        asyncio.run(run())
    elif args.component == "combined":
        asyncio.run(main())


if __name__ == "__main__":
    run()
