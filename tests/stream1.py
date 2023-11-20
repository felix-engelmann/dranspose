import time

import zmq
import itertools


class Acquisition:
    def __init__(self, socket, filename, it):
        self._socket = socket
        self._filename = filename
        self._msg_number = it

    async def start(self, meta=None):
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

    async def image(self, frame, shape, frameno, typ="uint16", compression="none"):
        before = time.perf_counter()
        await self._socket.send_json(
            {
                "htype": "image",
                "frame": frameno,
                "shape": shape,
                "type": typ,
                "compression": compression,
                "msg_number": next(self._msg_number),
            },
            flags=zmq.SNDMORE,
        )
        await self._socket.send(frame, copy=False)
        delay = time.perf_counter() - before
        if delay > 0.05:
            print("sending took", delay)

    async def close(self):
        await self._socket.send_json(
            {"htype": "series_end", "msg_number": next(self._msg_number)}
        )


class AcquisitionSocket:
    def __init__(self, ctx, bind):
        self.data_socket = ctx.socket(zmq.PUSH)
        self.data_socket.bind(bind)
        self.msg_number = itertools.count(0)

    async def start(self, filename, meta=None) -> Acquisition:
        print("return acq")
        acq = Acquisition(self.data_socket, filename, self.msg_number)
        await acq.start(meta)
        return acq

    async def close(self):
        self.data_socket.close()
