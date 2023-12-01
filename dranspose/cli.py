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
from dranspose.reducer import app as reducer_app
from dranspose.ingester import Ingester
from dranspose.ingesters.streaming_single import (
    StreamingSingleIngester,
    StreamingSingleSettings,
)
from dranspose.ingesters.streaming_contrast import (
    StreamingContrastIngester,
    StreamingContrastSettings,
)
from dranspose.ingesters.streaming_xspress3 import (
    StreamingXspressIngester,
    StreamingXspressSettings,
)
from dranspose.protocol import StreamName, WorkerName
from dranspose.worker import Worker


class CliSettings(BaseSettings):
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


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

    rconfig = uvicorn.Config(reducer_app, port=5001, log_level="info")
    rserver = uvicorn.Server(rconfig)
    reducer_task = asyncio.create_task(rserver.serve())

    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await server_task

    for i in ins + wos:
        await i.close()


def controller(args):
    try:
        config = uvicorn.Config(app, port=5000, host=args.host, log_level="info")
        server = uvicorn.Server(config)
        server.run()
    except KeyboardInterrupt:
        print("exiting")


def worker(args):
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


def ingester(args):
    print(args.ingestclass)
    ing = globals()[args.ingestclass]
    sett = globals()[args.ingestclass.replace("Ingester", "Settings")]

    async def run() -> None:
        i = ing(
            name=args.name,
            settings=sett(upstream_url=args.upstream_url),
        )
        await i.run()
        await i.close()

    asyncio.run(run())


def reducer(args):
    try:
        config = uvicorn.Config(
            reducer_app, port=5000, host=args.host, log_level="info"
        )
        server = uvicorn.Server(config)
        server.run()
    except KeyboardInterrupt:
        print("exiting")


def combined(args):
    asyncio.run(main())


def create_parser():
    parser = argparse.ArgumentParser(prog="dranspose", description="Transposes Streams")

    # Main command
    # parser.set_defaults(func=main_command)

    # Subcommands
    subparsers = parser.add_subparsers(title="commands", dest="subcommand")

    # Subcommand 1
    parser_ctrl = subparsers.add_parser("controller", help="run controller")
    parser_ctrl.set_defaults(func=controller)
    parser_ctrl.add_argument("--host", help="host to listen on")

    parser_reducer = subparsers.add_parser("reducer", help="run reducer")
    parser_reducer.set_defaults(func=reducer)
    parser_reducer.add_argument("--host", help="host to listen on")

    # Subcommand 2
    parser_worker = subparsers.add_parser("worker", help="run worker")
    parser_worker.set_defaults(func=worker)
    parser_worker.add_argument("-n", "--name", help="worker name (must not contain :)")

    parser_ingester = subparsers.add_parser("ingester", help="run ingester")
    parser_ingester.set_defaults(func=ingester)
    parser_ingester.add_argument(
        "-c", "--ingestclass", help="Ingester Class", required=True
    )
    parser_ingester.add_argument(
        "-u", "--upstream_url", help="Where to connect to upstream", required=True
    )
    parser_ingester.add_argument(
        "-n", "--name", help="Name of the ingester", required=True
    )

    parser_all = subparsers.add_parser(
        "combined", help="run all parts in a single process"
    )
    parser_all.set_defaults(func=combined)

    return parser


def run() -> None:
    parser = create_parser()
    args = parser.parse_args()

    # Check if a subcommand is provided
    if not getattr(args, "subcommand", None):
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    run()
