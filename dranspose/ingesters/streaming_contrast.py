import json
import pickle
from typing import AsyncGenerator, Optional

import zmq

from dranspose.data.contrast import (
    ContrastPacket,
    ContrastStarted,
    ContrastRunning,
    ContrastFinished,
)
from dranspose.event import StreamData
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import StreamName, ZmqUrl, IngesterName


class StreamingContrastSettings(IngesterSettings):
    upstream_url: ZmqUrl


class StreamingContrastIngester(Ingester):
    def __init__(
        self, name: StreamName, settings: Optional[StreamingContrastSettings] = None
    ) -> None:
        self._streaming_contrast_settings = settings
        if self._streaming_contrast_settings is None:
            self._streaming_contrast_settings = StreamingContrastSettings()

        super().__init__(
            IngesterName(f"{name}_ingester"), settings=self._streaming_contrast_settings
        )
        self.state.streams = [name]
        self.in_socket = self.ctx.socket(zmq.SUB)
        self.in_socket.connect(str(self._streaming_contrast_settings.upstream_url))
        self.in_socket.setsockopt(zmq.SUBSCRIBE, b"")

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        while True:
            self._logger.debug("clear up insocket")
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                data = pickle.loads(parts[0].bytes)
                packet = ContrastPacket.validate_python(data)
            except Exception as e:
                self._logger.warning("packet not valid %s", e.__repr__())
                continue
            self._logger.debug("received frame with header %s", packet)
            if type(packet) is ContrastStarted:
                self._logger.info("start of new sequence %s", packet)
                yield StreamData(typ="contrast", frames=parts)
                break
        while True:
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                data = pickle.loads(parts[0].bytes)
                packet = ContrastPacket.validate_python(data)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            if type(packet) is ContrastRunning:
                yield StreamData(typ="contrast", frames=parts)
            elif type(packet) is ContrastFinished:
                yield StreamData(typ="contrast", frames=parts)
                break
        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_socket.recv_multipart(copy=False)
