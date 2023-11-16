import asyncio
import json
import uuid

import redis.exceptions as rexceptions
import redis.asyncio as redis
import zmq.asyncio
import logging

from pydantic import UUID4

from dranspose.distributed import DistributedService
from dranspose.protocol import IngesterState, PREFIX, Stream, RedisKeys, WorkAssignment


class Ingester(DistributedService):
    def __init__(self, name: str, redis_host="localhost", redis_port=6379, config=None, **kwargs):
        super().__init__()
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
        streams: list[Stream] = []

        self.state = IngesterState(
            name = name,
            url = config.get(
                "worker_url", f"tcp://localhost:{config.get('worker_port', 10000)}"
            ),
            streams = streams
        )

    async def run(self):
        self.accept_task = asyncio.create_task(self.accept_workers())
        self.work_task = asyncio.create_task(self.work())
        self.assign_task = asyncio.create_task(self.manage_assignments())
        self.assignment_queue = asyncio.Queue()
        await self.register()

    async def restart_work(self, new_uuid: UUID4):
        self.work_task.cancel()
        self.assign_task.cancel()
        self.state.mapping_uuid = new_uuid
        self.assignment_queue = asyncio.Queue()
        self.work_task = asyncio.create_task(self.work())
        self.assign_task = asyncio.create_task(
            self.manage_assignments()
        )

    async def manage_assignments(self):
        self._logger.info("started ingester manage assign task")
        lastev = 0
        while True:
            sub = RedisKeys.assigned(self.state.mapping_uuid)
            try:
                assignments = await self.redis.xread({sub: lastev}, block=1000)
            except rexceptions.ConnectionError:
                break
            if sub not in assignments:
                continue
            assignment_evs = assignments[sub][0]
            self._logger.debug("got assignments %s", assignment_evs)
            for assignment in assignment_evs:
                wa = WorkAssignment.model_validate_json(assignment[1]["data"])
                mywa = wa.get_workers_for_streams(self.state.streams)
                await self.assignment_queue.put(mywa)
                lastev = assignment[0]

    async def work(self):
        self._logger.info("started ingester work task")
        sourcegens = {stream: self.run_source(stream) for stream in self.state.streams}
        while True:
            work_assignment: WorkAssignment = await self.assignment_queue.get()
            workermessages = {}
            zmqyields = []
            streams = []
            for stream in work_assignment.assignments:
                zmqyields.append(anext(sourcegens[stream]))
                streams.append(stream)
            zmqstreams = await asyncio.gather(*zmqyields)
            zmqparts = {stream: zmqpart for stream, zmqpart in zip(streams, zmqstreams)}
            for stream, workers in work_assignment.assignments.items():
                for worker in workers:
                    if worker not in workermessages:
                        workermessages[worker] = {
                            "data": [],
                            "header": {"event": work_assignment.event_number, "parts": []},
                        }
                    workermessages[worker]["data"] += zmqparts[stream]
                    workermessages[worker]["header"]["parts"].append(
                        {"stream": stream, "length": len(zmqparts[stream])}
                    )
            self._logger.debug("workermessages %s", workermessages)
            for worker, message in workermessages.items():
                await self.out_socket.send_multipart(
                    [worker.encode("ascii")]
                    + [json.dumps(message["header"]).encode("utf8")]
                    + message["data"]
                )
                self._logger.debug("sent message to worker %s", worker)

    async def run_source(self, stream):
        raise NotImplemented("get_frame must be implemented")

    async def accept_workers(self):
        poller = zmq.asyncio.Poller()
        poller.register(self.out_socket, zmq.POLLIN)
        while True:
            socks = dict(await poller.poll())
            for sock in socks:
                data = await sock.recv_multipart()
                self._logger.info("new worker connected %s", data[0])


    async def close(self):
        self.accept_task.cancel()
        self.work_task.cancel()
        await self.redis.delete(f"{PREFIX}:ingester:{self.state.name}:config")
        await self.redis.aclose()
        self.ctx.destroy(linger=0)
        self._logger.info("closed ingester")
