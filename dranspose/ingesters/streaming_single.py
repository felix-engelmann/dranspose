import json
from typing import AsyncGenerator, Optional

import zmq

from dranspose.data.stream1 import Stream1Packet, SeriesStart, SeriesData, SeriesEnd
from dranspose.event import StreamData
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import StreamName, ZmqUrl, IngesterName


class StreamingSingleSettings(IngesterSettings):
    upstream_url: ZmqUrl


class StreamingSingleIngester(Ingester):
    def __init__(
        self, name: StreamName, settings: Optional[StreamingSingleSettings] = None
    ) -> None:
        self._streaming_single_settings = settings
        if self._streaming_single_settings is None:
            self._streaming_single_settings = StreamingSingleSettings()

        super().__init__(
            IngesterName(f"{name}_ingester"), settings=self._streaming_single_settings
        )
        self.state.streams = [name]
        self.in_socket = self.ctx.socket(zmq.PULL)
        self.in_socket.connect(str(self._streaming_single_settings.upstream_url))

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        while True:
            self._logger.debug("clear up insocket")
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = Stream1Packet.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            self._logger.debug("received frame with header %s", packet)
            if type(packet) is SeriesStart:
                self._logger.info("start of new sequence %s", packet)
                yield StreamData(typ="STINS", frames=parts)
                break
        while True:
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = Stream1Packet.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            if type(packet) is SeriesData:
                yield StreamData(typ="STINS", frames=parts)
            elif type(packet) is SeriesEnd:
                yield StreamData(typ="STINS", frames=parts)
                break
        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_socket.recv_multipart(copy=False)
