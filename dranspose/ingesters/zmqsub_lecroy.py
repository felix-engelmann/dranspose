from typing import AsyncGenerator, Optional

import zmq

from dranspose.data.lecroy import (
    LecroyPacket,
    LecroyPrepare,
    LecroySeqEnd,
    LecroyStart,
    LecroyData,
    LecroyEnd,
)
from dranspose.event import StreamData
from dranspose.ingester import Ingester, IngesterSettings
from dranspose.protocol import StreamName, ZmqUrl


class ZmqSubLecroySettings(IngesterSettings):
    upstream_url: ZmqUrl


class ZmqSubLecroyIngester(Ingester):
    """
    In this protocol a frame does not correspond to a trigger
    if there are nch channels, a trigger will produce nch "traces" messages.
    There are 2 moddes

    Continuous mode
    m0, m1, tt..., m2, tt..., m2, tt..., m2, m3
    that is
        LecroyPrepare - LecroyStart
    followed by a lot of
        LecroyData - LecroyData... - LecroySeqEnd
    and ended by a
        LecroyEnd

    Sequential mode
    m0, m1, tt..., m2, m1, tt..., m2, m1, tt..., m2, [...],m2, m1, tt...,  m3
    that is
        LecroyPrepare
    followed by a lot of
        LecroyStart - LecroyData - LecroyData... - LecroySeqEnd
    and ended by a
        LecroyStart - LecroyData - LecroyData... - LecroyEnd

    Notes: I decided to cache the first LecroyStart received and to send it
    over and over at the beginning of each StreamData in continuos_mode
    so that the message structure would always be:
    LecroyStart - LecroyData... - LecroySeqEnd
    for both modes.
    Small ecception: the last StreamData in  Sequential mode will be
    LecroyStart - LecroyData... - LecroyEnd
    the LecroyEnd packet is then repeated on its own.
    """

    def __init__(self, settings: Optional[ZmqSubLecroySettings] = None) -> None:
        if settings is not None:
            self._streaming_Lecroy_settings = settings
        else:
            self._streaming_Lecroy_settings = ZmqSubLecroySettings()

        super().__init__(settings=self._streaming_Lecroy_settings)
        self.in_socket: Optional[zmq._future._AsyncSocket] = None

    async def run_source(self, stream: StreamName) -> AsyncGenerator[StreamData, None]:
        self.in_socket = self.ctx.socket(zmq.SUB)
        self.in_socket.connect(str(self._streaming_Lecroy_settings.upstream_url))
        self.in_socket.setsockopt(zmq.SUBSCRIBE, b"")
        self._logger.info(
            "subscribed to %s", self._streaming_Lecroy_settings.upstream_url
        )

        while True:
            self._logger.debug("clear up insocket")
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = LecroyPacket.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.warning("packet not valid %s", e.__repr__())
                continue
            self._logger.debug(
                f"received frame of type {type(packet)} with header {packet}"
            )
            if type(packet) is LecroyPrepare:
                self._logger.info("start of new sequence %s", packet)
                parts = await self.in_socket.recv_multipart(copy=False)
                try:
                    packet = LecroyPacket.validate_json(parts[0].bytes)
                except Exception as e:
                    self._logger.warning("packet not valid %s", e.__repr__())
                    continue
                if type(packet) is LecroyStart:
                    self._logger.info("start of new sequence %s", packet)
                    yield StreamData(typ="Lecroy", frames=parts)
                    break
        continuos_mode = packet.ntriggers == -1
        frames = parts  # LecroyStart0
        while True:
            parts = await self.in_socket.recv_multipart(copy=False)
            try:
                packet = LecroyPacket.validate_json(parts[0].bytes)
            except Exception as e:
                self._logger.error("packet not valid %s", e.__repr__())
                continue
            self._logger.debug(
                f"received frame of type {type(packet)} with header {packet}"
            )
            if continuos_mode:
                # LecroyStart0 has been already received
                # receive LecroyData - LecroyData... - LecroySeqEnd
                # then send LecroyStart0 - LecroyData... - LecroySeqEnd
                if isinstance(packet, LecroyData):
                    frames += parts
                elif isinstance(packet, LecroySeqEnd):
                    frames += parts
                    yield StreamData(typ="Lecroy", frames=frames)
                    frames = [frames[0]]  # keep LecroyStart0
                elif isinstance(packet, LecroyEnd):
                    self._logger.info("reached end %s", packet)
                    if len(frames) != 1:
                        self._logger.error(
                            "Untrasmitted frames left in the buffer %s",
                            frames.__repr__(),
                        )
                    yield StreamData(typ="Lecroy", frames=parts)
                    break
            else:
                # receive LecroyData - LecroyData... - LecroySeqEnd
                # then send LecroyStart - LecroyData - LecroyData... - LecroySeqEnd
                # receive LecroyStart
                if isinstance(packet, LecroyStart) or isinstance(packet, LecroyData):
                    frames += parts
                elif isinstance(packet, LecroySeqEnd):
                    frames += parts
                    yield StreamData(typ="Lecroy", frames=frames)
                    frames = []  # empty buffer
                elif isinstance(packet, LecroyEnd):
                    self._logger.info("reached end %s", packet)
                    frames += parts
                    yield StreamData(typ="Lecroy", frames=frames)
                    # send end-message separately
                    yield StreamData(typ="Lecroy", frames=parts)
                    break

        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_socket.recv_multipart(copy=False)

    async def stop_source(self, stream: StreamName) -> None:
        if self.in_socket:
            self._logger.info("closing socket without linger")
            self.in_socket.close(linger=0)
            self.in_socket = None
