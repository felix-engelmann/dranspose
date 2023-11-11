import asyncio
import json
import time
from typing import Dict, List

import redis.exceptions as rexceptions
import redis.asyncio as redis
import zmq.asyncio
import logging
from dranspose import protocol


class IngesterState:
    def __init__(self, name: str, url: str, streams: List[str]):
        self.name = name
        self.url = url
        self.streams = streams
        self.mapping_uuid = None


class Ingester:
    def __init__(self, name: str, redis_host="localhost", redis_port=6379, config=None):
        self._logger = logging.getLogger(f"{__name__}+{name}")
        if config is None:
            config = {}
        if ":" in name:
            raise Exception("Worker name must not container a :")
        self.ctx = zmq.asyncio.Context()
        self.out_socket = self.ctx.socket(zmq.ROUTER)
        self.out_socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.out_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 300)
        self.out_socket.bind(f"tcp://*:{config.get('worker_port', 10000)}")
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True, protocol=3
        )
        streams: List[str] = []

        self.state = IngesterState(
            name,
            config.get(
                "worker_url", f"tcp://localhost:{config.get('worker_port', 10000)}"
            ),
            streams,
        )

        asyncio.create_task(self.register())
        asyncio.create_task(self.accept_workers())
        self.work_task = asyncio.create_task(self.work())
        self.assign_task = asyncio.create_task(self.manage_assignments())
        self.assignment_queue = asyncio.Queue()

    async def manage_assignments(self):
        self._logger.info("started ingester manage assign task")
        lastev = 0
        while True:
            sub = f"{protocol.PREFIX}:assigned:{self.state.mapping_uuid}"
            try:
                assignments = await self.redis.xread({sub: lastev}, block=1000)
            except rexceptions.ConnectionError:
                break
            if sub not in assignments:
                continue
            assignment_evs = assignments[sub][0]
            self._logger.debug("got assignments %s", assignment_evs)
            for assignment in assignment_evs:
                assigned_workers = {"event":int(assignment[0].split("-")[0]),"streams":{}}
                for stream in self.state.streams:
                    if stream in assignment[1]:
                        workers = json.loads(assignment[1][stream])
                        assigned_workers["streams"][stream] = workers
                await self.assignment_queue.put(assigned_workers)
                lastev = assignment[0]

    async def work(self):
        self._logger.info("started ingester work task")
        sourcegens = {stream: self.run_source(stream) for stream in self.state.streams}
        while True:
            assigned_workers = await self.assignment_queue.get()
            workermessages = {}
            zmqyields = []
            streams = []
            for stream in assigned_workers["streams"]:
                zmqyields.append(anext(sourcegens[stream]))
                streams.append(stream)
            zmqstreams = await asyncio.gather(*zmqyields)
            zmqparts = {stream:zmqpart for stream, zmqpart in zip(streams, zmqstreams)}
            for stream, workers in assigned_workers["streams"].items():
                for worker in workers:
                    if worker not in workermessages:
                        workermessages[worker] = []
                    workermessages[worker] += zmqparts[stream]
            self._logger.debug("workermessages %s", workermessages)
            for worker, message in workermessages.items():
                await self.out_socket.send_multipart([worker.encode("ascii")]+[f'event no {assigned_workers["event"]}'.encode("ascii")] + message)

    async def run_source(self, stream):
        raise NotImplemented("get_frame must be implemented")

    async def handle_frame(self, frameno, parts):
        pass

    async def accept_workers(self):
        poller = zmq.asyncio.Poller()
        poller.register(self.out_socket, zmq.POLLIN)
        while True:
            socks = dict(await poller.poll())
            for sock in socks:
                data = await sock.recv_multipart()
                self._logger.info("new worker connected %s", data[0])

    async def register(self):
        latest = await self.redis.xrevrange(
            f"{protocol.PREFIX}:controller:updates", count=1
        )
        last = 0
        if len(latest) > 0:
            last = latest[0][0]
        while True:
            await self.redis.setex(
                f"{protocol.PREFIX}:ingester:{self.state.name}:present", 10, 1
            )
            await self.redis.json().set(
                f"{protocol.PREFIX}:ingester:{self.state.name}:config",
                "$",
                self.state.__dict__,
            )
            try:
                update = await self.redis.xread(
                    {f"{protocol.PREFIX}:controller:updates": last}, block=6000
                )
                if f"{protocol.PREFIX}:controller:updates" in update:
                    update = update[f"{protocol.PREFIX}:controller:updates"][0][-1]
                    last = update[0]
                    newuuid = update[1]["mapping_uuid"]
                    if newuuid != self.state.mapping_uuid:
                        self._logger.info("resetting config to %s", newuuid)
                        self.work_task.cancel()
                        self.assign_task.cancel()
                        self.state.mapping_uuid = newuuid
                        self.assignment_queue = asyncio.Queue()
                        self.work_task = asyncio.create_task(self.work())
                        self.assign_task = asyncio.create_task(self.manage_assignments())
            except rexceptions.ConnectionError as e:
                break

    async def close(self):
        await self.redis.delete(f"{protocol.PREFIX}:ingester:{self.state.name}:config")
        await self.redis.aclose()
        self.ctx.destroy(linger=0)
        self._logger.info("closed ingester")
