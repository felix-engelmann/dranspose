from typing import AsyncGenerator, Optional

import zmq

from dranspose.data.eiger_legacy import (
    EigerLegacyPacket,
    EigerLegacyHeader,
    EigerLegacyImage,
    EigerLegacyEnd,
)
from dranspose.event import StreamData
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import StreamName, ZmqUrl


class ZmqPullEigerLegacySettings(IngesterSettings):
    upstream_url: ZmqUrl


class ZmqPullEigerLegacyIngester(Ingester):
    """
    A simple ingester class to comsume a stream from the streaming-receiver repub port
    """

    def __init__(self, settings: Optional[ZmqPullEigerLegacySettings] = None) -> None:
        if settings is not None:
            self._streaming_eiger_legacy_settings = settings
        else:
            self._streaming_eiger_legacy_settings = ZmqPullEigerLegacySettings()

        super().__init__(settings=self._streaming_eiger_legacy_settings)
        self.in_socket: Optional[zmq._future._AsyncSocket] = None

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        self.in_socket = self.ctx.socket(zmq.PULL)
        self.in_socket.connect(str(self._streaming_eiger_legacy_settings.upstream_url))
        self._logger.info(
            "pulling from %s", self._streaming_eiger_legacy_settings.upstream_url
        )

        while True:
            self._logger.debug("clear up insocket")
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = EigerLegacyPacket.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            self._logger.debug("received frame with header %s", packet)
            if isinstance(packet, EigerLegacyHeader):
                self._logger.info("start of new sequence %s", packet)
                yield StreamData(typ="EIGER_LEGACY", frames=parts)
                break
        while True:
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = EigerLegacyPacket.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            if isinstance(packet, EigerLegacyImage):
                yield StreamData(typ="EIGER_LEGACY", frames=parts)
            elif isinstance(packet, EigerLegacyEnd):
                yield StreamData(typ="EIGER_LEGACY", frames=parts)
                break
        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_socket.recv_multipart(copy=False)

    async def stop_source(self, stream: StreamName) -> None:
        if self.in_socket:
            self._logger.info("closing socket without linger")
            self.in_socket.close(linger=0)
            self.in_socket = None
