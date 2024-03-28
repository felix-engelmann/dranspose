import argparse
import asyncio
import json
import logging
import os
import signal
from asyncio import Task
from typing import Literal, Any, Optional

import uvicorn
from pydantic_core import Url
from pydantic_settings import BaseSettings

from dranspose.controller import app
from dranspose.helpers.utils import done_callback
from dranspose.reducer import app as reducer_app
from dranspose.debug_worker import app as debugworker_app
from dranspose.ingesters import *  # noqa: F403, F401
from dranspose.ingesters.zmqpull_single import (
    ZmqPullSingleIngester,
    ZmqPullSingleSettings,
)
from dranspose.protocol import StreamName, WorkerName
from dranspose.worker import Worker, WorkerSettings

from dranspose.replay import replay as run_replay


class CliSettings(BaseSettings):
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


settings = CliSettings()

logging.basicConfig(level=settings.log_level.upper())


async def main() -> None:
    ins = []
    ins.append(
        ZmqPullSingleIngester(
            settings=ZmqPullSingleSettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )
    wos = [
        Worker(WorkerSettings(worker_name=WorkerName("worker" + str(i))))
        for i in range(1, 3)
    ]

    for i in ins + wos:
        t = asyncio.create_task(i.run())
        t.add_done_callback(done_callback)

    rconfig = uvicorn.Config(reducer_app, port=5001, log_level="info")
    rserver = uvicorn.Server(rconfig)
    tr = asyncio.create_task(rserver.serve())
    tr.add_done_callback(done_callback)

    config = uvicorn.Config(app, port=5000, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await server_task

    for i in ins + wos:
        await i.close()


def controller(args: argparse.Namespace) -> None:
    try:
        config = uvicorn.Config(app, port=5000, host=args.host, log_level="info")
        server = uvicorn.Server(config)
        server.run()
    except KeyboardInterrupt:
        print("exiting")


worker_task: Optional[Task[None]] = None


def worker(args: argparse.Namespace) -> None:
    def stop(*args: Any) -> None:
        global worker_task
        if worker_task:
            worker_task.cancel()

    async def run() -> None:
        global worker_task
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, stop)

        if args.name is None:
            settings = WorkerSettings(worker_class=args.workerclass)
        else:
            settings = WorkerSettings(
                worker_name=args.name, worker_class=args.workerclass
            )

        w = Worker(settings)
        worker_task = asyncio.create_task(w.run())
        await worker_task
        await w.close()

    asyncio.run(run())


ingester_task = None


def ingester(args: argparse.Namespace) -> None:
    print(args.ingesterclass)
    ing = globals()[args.ingesterclass]
    sett = globals()[args.ingesterclass.replace("Ingester", "Settings")]

    def stop(*args: Any) -> None:
        global ingester_task
        if ingester_task:
            ingester_task.cancel()

    async def run() -> None:
        global ingester_task
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, stop)

        i = ing(
            settings=sett(upstream_url=args.upstream_url, ingester_streams=[args.name]),
        )
        ingester_task = asyncio.create_task(i.run())
        await ingester_task
        await i.close()

    asyncio.run(run())


def reducer(args: argparse.Namespace) -> None:
    try:
        if args.reducerclass:
            os.environ["REDUCER_CLASS"] = args.reducerclass
        config = uvicorn.Config(
            reducer_app, port=5002, host=args.host, log_level="info"
        )
        server = uvicorn.Server(config)
        server.run()
    except KeyboardInterrupt:
        print("exiting")


def http_ingester(args: argparse.Namespace) -> None:
    try:
        os.environ["INGESTER_STREAMS"] = json.dumps([args.name])
        config = uvicorn.Config(
            args.ingesterapp, port=args.port or 5000, host=args.host, log_level="info"
        )
        server = uvicorn.Server(config)
        server.run()
    except KeyboardInterrupt:
        print("exiting")


def debugworker(args: argparse.Namespace) -> None:
    try:
        if args.name:
            os.environ["WORKER_NAME"] = args.name
        config = uvicorn.Config(
            debugworker_app, port=5000, host=args.host, log_level="info"
        )
        server = uvicorn.Server(config)
        server.run()
    except KeyboardInterrupt:
        print("exiting")


def combined(args: argparse.Namespace) -> None:
    asyncio.run(main())


def replay(args: argparse.Namespace) -> None:
    run_replay(
        args.workerclass,
        args.reducerclass,
        args.files,
        args.parameters,
        args.port,
        args.keep_alive,
        args.nworkers,
    )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dranspose", description="Transposes Streams")

    subparsers = parser.add_subparsers(title="commands", dest="subcommand")

    parser_ctrl = subparsers.add_parser("controller", help="run controller")
    parser_ctrl.set_defaults(func=controller)
    parser_ctrl.add_argument("--host", help="host to listen on")

    parser_reducer = subparsers.add_parser("reducer", help="run reducer")
    parser_reducer.set_defaults(func=reducer)
    parser_reducer.add_argument("--host", help="host to listen on")
    parser_reducer.add_argument(
        "-c",
        "--reducerclass",
        help="reducer class e.g. 'src.reducer:FluorescenceReducer'",
    )

    parser_debugworker = subparsers.add_parser("debugworker", help="run debugworker")
    parser_debugworker.set_defaults(func=debugworker)
    parser_debugworker.add_argument("--host", help="host to listen on")
    parser_debugworker.add_argument(
        "-n",
        "--name",
        help="debug worker name",
    )

    parser_worker = subparsers.add_parser("worker", help="run worker")
    parser_worker.set_defaults(func=worker)
    parser_worker.add_argument("-n", "--name", help="worker name (must not contain :)")
    parser_worker.add_argument(
        "-c", "--workerclass", help="worker class e.g. 'src.worker:FluorescenceWorker'"
    )

    parser_ingester = subparsers.add_parser("ingester", help="run ingester")
    parser_ingester.set_defaults(func=ingester)
    parser_ingester.add_argument(
        "-c", "--ingesterclass", help="Ingester Class", required=True
    )
    parser_ingester.add_argument(
        "-u", "--upstream_url", help="Where to connect to upstream", required=True
    )
    parser_ingester.add_argument(
        "-n", "--name", help="Name of the ingester", required=True
    )

    parser_http_ingester = subparsers.add_parser(
        "http_ingester", help="run http ingester"
    )
    parser_http_ingester.set_defaults(func=http_ingester)
    parser_http_ingester.add_argument("--host", help="host to listen on")
    parser_http_ingester.add_argument(
        "-c", "--ingesterapp", help="Ingester App", required=True
    )
    parser_http_ingester.add_argument("-p", "--port", help="Where to listen on")
    parser_http_ingester.add_argument(
        "-n", "--name", help="Name of the ingester", required=True
    )

    parser_all = subparsers.add_parser(
        "combined", help="run all parts in a single process"
    )
    parser_all.set_defaults(func=combined)

    parser_replay = subparsers.add_parser(
        "replay", help="run replay of ingester recorded files"
    )
    parser_replay.set_defaults(func=replay)
    parser_replay.add_argument(
        "-w",
        "--workerclass",
        help="worker class e.g. 'src.worker:FluorescenceWorker'",
        required=True,
    )
    parser_replay.add_argument(
        "-r",
        "--reducerclass",
        help="reducer class e.g. 'src.reducer:FluorescenceReducer'",
        required=True,
    )
    parser_replay.add_argument(
        "-f", "--files", nargs="+", help="List of files to replay", required=True
    )
    parser_replay.add_argument(
        "-p", "--parameters", help="parameter file, json or pickle"
    )
    parser_replay.add_argument(
        "--port", type=int, help="expose the reducer result via http on port"
    )
    parser_replay.add_argument(
        "--keep-alive", action="store_true", help="keep the server alive after finish"
    )
    parser_replay.add_argument(
        "--nworkers",
        help="number of workers to distribute events to",
        default=1,
        type=int,
    )

    return parser


def run() -> None:
    parser = create_parser()
    args: argparse.Namespace = parser.parse_args()

    # Check if a subcommand is provided
    if not getattr(args, "subcommand", None):
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    run()
