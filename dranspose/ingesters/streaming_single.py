import json

import numpy as np
import zmq

from dranspose.ingester import Ingester


class StreamingSingleIngester(Ingester):
    def __init__(self, name, connect_url, worker_port=10010, **kwargs):
        config = {"worker_port": worker_port}
        if kwargs.get("worker_url") is not None:
            config["worker_url"] = kwargs["worker_url"]
        super().__init__(f"{name}_ingester", config=config, **kwargs)
        self.state.streams = [name]
        self.in_socket = self.ctx.socket(zmq.PULL)
        self.in_socket.connect(connect_url)

    async def run_source(self, stream):
        hdr = None
        while True:
            self._logger.debug("clear up insocket")
            parts = await self.in_socket.recv_multipart(copy=False)
            header = json.loads(parts[0].bytes)
            self._logger.debug("received frame with header %s", header)
            if header["htype"] == "header":
                self._logger.info("start of new sequence %s", header)
                hdr = parts[0]
                break
        while True:
            parts = await self.in_socket.recv_multipart(copy=False)
            header = json.loads(parts[0].bytes)
            if header["htype"] == "image":
                yield [hdr] + parts
            if header["htype"] == "series_end":
                break
        while True:
            self._logger.debug("discarding messages until next run")
            await self.in_socket.recv_multipart(copy=False)
