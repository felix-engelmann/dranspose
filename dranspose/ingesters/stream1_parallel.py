from typing import Optional, AsyncGenerator
import zmq

from dranspose.data.eiger_legacy import (
    EigerLegacyPacket,
    EigerLegacyEnd,
    EigerLegacyImage,
    EigerLegacyHeader,
)
from dranspose.event import InternalWorkerMessage, StreamData
from dranspose.ingesters.stins_parallel import StinsParallelIngester
from dranspose.ingesters.zmqpull_eiger_legacy import ZmqPullEigerLegacySettings
from dranspose.protocol import StreamName, EventNumber


class Stream1ParallelSettings(ZmqPullEigerLegacySettings):
    pass


class Stream1ParallelIngester(StinsParallelIngester):
    def __init__(self, settings: Optional[Stream1ParallelSettings] = None) -> None:
        if settings is not None:
            self._streaming_settings = settings
        else:
            self._streaming_settings = ZmqPullEigerLegacySettings()

        super().__init__(settings=self._streaming_settings)
        self.in_socket: Optional[zmq._future._AsyncSocket] = None

    async def run_source_part(
        self, stream: StreamName
    ) -> AsyncGenerator[InternalWorkerMessage, None]:
        self.in_socket = self.ctx.socket(zmq.PULL)
        self.in_socket.connect(str(self._streaming_settings.upstream_url))
        self._logger.info("pulling from %s", self._streaming_settings.upstream_url)

        while True:
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = EigerLegacyPacket.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            msg_number = None
            if isinstance(packet, EigerLegacyImage):
                msg_number = EventNumber(packet.frame + 1)
            elif isinstance(packet, EigerLegacyHeader):
                msg_number = EventNumber(0)
            elif isinstance(packet, EigerLegacyEnd):
                break
            self._logger.debug("msg number %d", msg_number)
            yield InternalWorkerMessage(
                event_number=msg_number,
                streams={stream: StreamData(typ="EIGER_LEGACY", frames=parts)},
            )

        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_socket.recv_multipart(copy=False)
