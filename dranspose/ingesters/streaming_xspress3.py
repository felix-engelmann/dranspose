import json
import pickle
from typing import AsyncGenerator, Optional

import zmq

from dranspose.data.xspress3 import (
    XspressPacket,
    XspressStart,
    XspressImage,
    XspressEnd,
)
from dranspose.event import StreamData
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import StreamName, ZmqUrl, IngesterName


class StreamingXspressSettings(IngesterSettings):
    upstream_url: ZmqUrl


class StreamingXspressIngester(Ingester):
    def __init__(
        self, name: StreamName, settings: Optional[StreamingXspressSettings] = None
    ) -> None:
        self._streaming_xspress_settings = settings
        if self._streaming_xspress_settings is None:
            self._streaming_xspress_settings = StreamingXspressSettings()

        super().__init__(
            IngesterName(f"{name}_ingester"), settings=self._streaming_xspress_settings
        )
        self.state.streams = [name]
        self.in_socket = self.ctx.socket(zmq.SUB)
        self.in_socket.connect(str(self._streaming_xspress_settings.upstream_url))
        self.in_socket.setsockopt(zmq.SUBSCRIBE, b"")

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        while True:
            self._logger.debug("clear up insocket")
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = XspressPacket.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.warning("packet not valid %s", e.__repr__())
                continue
            self._logger.debug("received frame with header %s", packet)
            if type(packet) is XspressStart:
                self._logger.info("start of new sequence %s", packet)
                yield StreamData(typ="xspress", frames=parts)
                break
        while True:
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = XspressPacket.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            if type(packet) is XspressImage:
                image = await self.in_socket.recv_multipart(copy=False)
                meta = await self.in_socket.recv_multipart(copy=False)
                yield StreamData(typ="xspress", frames=parts + image + meta)
            elif type(packet) is XspressEnd:
                yield StreamData(typ="xspress", frames=parts)
                break
        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_socket.recv_multipart(copy=False)
