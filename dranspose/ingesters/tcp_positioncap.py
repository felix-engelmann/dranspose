import asyncio
from asyncio import StreamWriter
from typing import AsyncGenerator, Optional

import zmq
from pydantic_core import Url

from dranspose.event import StreamData
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import StreamName


class TcpPcapSettings(IngesterSettings):
    upstream_url: Url


class TcpPcapIngester(Ingester):
    """
    A simple ingester class to comsume a stream from the streaming-receiver repub port
    """

    def __init__(self, settings: Optional[TcpPcapSettings] = None) -> None:
        if settings is not None:
            self._pcap_settings = settings
        else:
            self._pcap_settings = TcpPcapSettings()

        super().__init__(settings=self._pcap_settings)
        self.writer: Optional[StreamWriter] = None

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        reader, self.writer = await asyncio.open_connection(
            self._pcap_settings.upstream_url.host, self._pcap_settings.upstream_url.port
        )
        self.writer.write(b"\n")
        data = await reader.readline()
        assert data == b"OK\n"
        stop = False
        self._logger.info("connected to %s", self._pcap_settings.upstream_url)

        while True:
            while True:
                self._logger.debug("clear up insocket")
                line = await reader.readline()
                self._logger.debug("received line %s", line)
                if line == b"":
                    stop = True
                    break
                if not line.startswith(b"arm_time: "):
                    self._logger.debug("discard non-start packet")
                else:
                    self._logger.info("start of new sequence %s", line)
                    header = [line]
                    while line := await reader.readline():
                        header.append(line)
                        if line.strip() == b"":
                            break
                    self._logger.info("use header %s", header)
                    frame = zmq.Frame(b"".join(header))
                    yield StreamData(typ="PCAP_RAW", frames=[frame])
                    break
            if stop:
                break
            while True:
                line = await reader.readline()
                self._logger.debug("send frame %s", line)
                frame = zmq.Frame(line)
                yield StreamData(typ="PCAP_RAW", frames=[frame])
                if line.startswith(b"END"):
                    break

        while True:
            self._logger.debug("discarding messages until next run")
            line = await reader.readline()
            if line == b"":
                break

    async def stop_source(self, stream: StreamName) -> None:
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.writer = None
