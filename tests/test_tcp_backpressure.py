import logging
from asyncio import StreamReader, StreamWriter
from typing import Callable, Coroutine

import pytest
import asyncio

from dranspose.helpers.utils import cancel_and_wait


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    request = (await reader.read(255)).decode("utf8")
    if request.strip() != "":
        logging.error("bad initial hello %s", request)
    writer.write(b"OK\n")
    await writer.drain()
    for i in range(1000):
        writer.write(b"testdata" * 1000 + b"\n")
        await writer.drain()
        if i % 100 == 0:
            logging.info("sent out %d", i)
        # await asyncio.sleep(0.001)

    await writer.drain()
    writer.close()


@pytest.mark.asyncio
async def test_tcp(stream_pcap: Callable[[int], Coroutine[None, None, None]]):
    server = await asyncio.start_server(handle_client, "localhost", 8889)
    task = asyncio.create_task(server.serve_forever())

    reader, writer = await asyncio.open_connection("127.0.0.1", 8889)
    writer.write(b"\n")
    data = await reader.readline()
    assert data == b"OK\n"
    logging.info("connected to %s", "127.0.0.1")

    # for i in range(1000):
    #    line = await reader.readline()
    #    logging.debug("line is %s", line)
    await asyncio.sleep(1)

    await cancel_and_wait(task)
