import asyncio
from typing import AsyncGenerator, Optional

import zmq


from dranspose.data.stream1 import Stream1Packet, Stream1End
from dranspose.event import StreamData, InternalWorkerMessage
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import (
    StreamName,
    ZmqUrl,
    WorkAssignment,
    WorkerName,
)


class StinsParallelSettings(IngesterSettings):
    upstream_url: ZmqUrl


class StinsParallelIngester(Ingester):
    """
    A simple ingester class to comsume a stream from the streaming-receiver repub port
    """

    def __init__(self, settings: Optional[StinsParallelSettings] = None) -> None:
        if settings is not None:
            self._streaming_single_settings = settings
        else:
            self._streaming_single_settings = StinsParallelSettings()

        super().__init__(settings=self._streaming_single_settings)
        self.in_socket: Optional[zmq._future._AsyncSocket] = None

    async def work(self) -> None:
        self._logger.info("started stins ingester work task")

        if len(self.active_streams) == 0:
            self._logger.warning("this ingester has no active streams, stopping worker")
            return
        sourcegen = self.run_source_part(self.active_streams[0])
        try:
            while True:
                nextiwm: InternalWorkerMessage = await anext(sourcegen)

                work_assignment: WorkAssignment = await self.assignment_queue.get()
                while work_assignment.event_number < nextiwm.event_number:
                    work_assignment = await self.assignment_queue.get()

                workermessages: dict[WorkerName, InternalWorkerMessage] = {}
                for stream, workers in work_assignment.assignments.items():
                    for worker in workers:
                        if worker not in workermessages:
                            workermessages[worker] = nextiwm
                self._logger.debug("workermessages %s", workermessages)
                await self._send_workermessages(workermessages)
                self.state.processed_events += 1
        except asyncio.exceptions.CancelledError:
            self._logger.info("stopping worker")
            for stream in self.active_streams:
                await self.stop_source(stream)

    async def run_source_part(
        self, stream: StreamName
    ) -> AsyncGenerator[InternalWorkerMessage, None]:
        self.in_socket = self.ctx.socket(zmq.PULL)
        self.in_socket.connect(str(self._streaming_single_settings.upstream_url))
        self._logger.info(
            "pulling from %s", self._streaming_single_settings.upstream_url
        )

        while True:
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = Stream1Packet.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            self._logger.debug("msg number %d", packet.msg_number)
            yield InternalWorkerMessage(
                event_number=packet.msg_number,
                streams={stream: StreamData(typ="STINS", frames=parts)},
            )

            if isinstance(packet, Stream1End):
                break
        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_socket.recv_multipart(copy=False)

    async def stop_source(self, stream: StreamName) -> None:
        if self.in_socket:
            self._logger.info("closing socket without linger")
            self.in_socket.close(linger=0)
            self.in_socket = None
