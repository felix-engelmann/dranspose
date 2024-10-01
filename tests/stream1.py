import time
from typing import Any, Iterator, Optional

import zmq
import itertools

from numpy import ndarray

from dranspose.protocol import ZmqUrl


class Acquisition:
    def __init__(self, socket: Any, filename: str, it: Iterator[int]):
        self._socket = socket
        self._filename = filename
        self._msg_number = it

    async def start(self, meta: Any = None) -> None:
        header = {
            "htype": "header",
            "filename": self._filename,
            "msg_number": next(self._msg_number),
        }
        if not meta:
            await self._socket.send_json(header)
        else:
            await self._socket.send_json(header, flags=zmq.SNDMORE)
            await self._socket.send_json(meta)

    async def image(
        self,
        frame: zmq.Frame | ndarray[Any, Any],
        shape: tuple[int, ...],
        frameno: int,
        typ: str = "uint16",
        compression: str = "none",
        extra_fields: Optional[dict[str, Any]] = None,
    ) -> None:
        before = time.perf_counter()
        if extra_fields is None:
            extra_fields = {}
        await self._socket.send_json(
            {
                "htype": "image",
                "frame": frameno,
                "shape": shape,
                "type": typ,
                "compression": compression,
                "msg_number": next(self._msg_number),
                **extra_fields,
            },
            flags=zmq.SNDMORE,
        )
        await self._socket.send(frame, copy=False)
        delay = time.perf_counter() - before
        if delay > 0.05:
            print("sending took", delay)

    async def close(self) -> None:
        await self._socket.send_json(
            {"htype": "series_end", "msg_number": next(self._msg_number)}
        )


class AcquisitionSocket:
    def __init__(
        self, ctx: zmq.Context[Any], bind: ZmqUrl, typ: int = zmq.PUSH
    ) -> None:
        self.data_socket = ctx.socket(typ)
        self.data_socket.bind(str(bind))
        self.msg_number = itertools.count(0)

    async def start(self, filename: str, meta: Any = None) -> Acquisition:
        print("return acq")
        acq = Acquisition(self.data_socket, filename, self.msg_number)
        await acq.start(meta)
        return acq

    async def close(self) -> None:
        self.data_socket.close()
