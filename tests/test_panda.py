import asyncio
import logging
from typing import Callable, Coroutine

import pytest


@pytest.mark.asyncio
async def test_panda_pcap(
    stream_pcap: Callable[[int], Coroutine[None, None, None]],
) -> None:
    ntrig = 10
    asyncio.create_task(stream_pcap(ntrig - 1))
    await asyncio.sleep(0.5)
    reader, writer = await asyncio.open_connection("localhost", 8889)
    writer.write(b"\n")
    data = await reader.readline()
    assert data == b"OK\n"
    while data := await reader.readline():
        logging.info("got line %s", data)
        if data.startswith(b"END"):
            break
    writer.close()
    await writer.wait_closed()

    await asyncio.sleep(1)
