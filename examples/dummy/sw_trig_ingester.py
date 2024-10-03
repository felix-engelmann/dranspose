import json
from typing import AsyncGenerator, Any, Iterator

import zmq

from dranspose.event import StreamData
from dranspose.ingester import IsSoftwareTriggered
from dranspose.ingesters.tcp_positioncap import TcpPcapIngester
from dranspose.protocol import StreamName


class SoftTriggerPcapIngester(TcpPcapIngester):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    async def run_source(
        self, stream: StreamName
    ) -> AsyncGenerator[StreamData | IsSoftwareTriggered, None]:
        if stream == "dummy":
            while True:
                yield IsSoftwareTriggered()
        async for streamdata in super().run_source(stream):
            yield streamdata

    def software_trigger(self) -> Iterator[dict[StreamName, StreamData]]:
        while True:
            data = {"temperature": 315.15}
            self._logger.debug("send data %s", data)
            frame = zmq.Frame(json.dumps(data).encode("utf8"))
            yield {StreamName("dummy"): StreamData(typ="JSON", frames=[frame])}
