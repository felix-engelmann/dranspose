import asyncio
import logging
import os
from pathlib import PosixPath
from typing import Callable, Coroutine, Any

import zmq.asyncio

import pytest

from dranspose.helpers.utils import cancel_and_wait
from tests.utils import consume_zmq


@pytest.mark.asyncio
async def test_cbor(
    stream_cbors: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
) -> None:
    with zmq.asyncio.Context() as context:
        task = asyncio.create_task(consume_zmq(context, 9, typ=zmq.PULL, port=22004))

        asyncio.create_task(
            stream_cbors(
                context,
                22004,
                PosixPath("tests/data/albaem-dump.cbors"),
                0.001,
                zmq.PUSH,
            )
        )

        await task


@pytest.mark.parametrize(
    "begin,end",
    [
        (3, None),
        (None, 4),
        (2, 5),
    ],
)
@pytest.mark.asyncio
async def test_cbor_begin(
    stream_cbors: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int, int, int],
        Coroutine[Any, Any, None],
    ],
    begin,
    end,
) -> None:
    with zmq.asyncio.Context() as context:
        file_content = [
            b'{"version": 1, "message_type": "series-start", "message_id": 1}',
            b'{"version": 1, "message_type": "data", "message_id": 2, "frame_number": 0, "timestamp": 7.72645192, "acquisition_timestamp": 1704807770443944912, "channel1": -2.8302592615927417e-11, "channel2": -6.091210149949596e-11, "channel3": 2.5349278603830644e-10, "channel4": 4.80528800718246e-10}',
            b'{"version": 1, "message_type": "data", "message_id": 3, "frame_number": 1, "timestamp": 10.86213192, "acquisition_timestamp": 1704807773579624912, "channel1": -2.399567634828629e-11, "channel2": -7.260230279737902e-11, "channel3": 2.4918586977066537e-10, "channel4": 4.866815382434476e-10}',
            b'{"version": 1, "message_type": "data", "message_id": 4, "frame_number": 2, "timestamp": 14.84581192, "acquisition_timestamp": 1704807777563304912, "channel1": -2.2149855090725806e-11, "channel2": -7.813976657006048e-11, "channel3": 2.621066185735887e-10, "channel4": 4.916037282636089e-10}',
            b'{"version": 1, "message_type": "data", "message_id": 5, "frame_number": 3, "timestamp": 20.66757192, "acquisition_timestamp": 1704807783385064912, "channel1": -9.84438004032258e-12, "channel2": -6.091210149949596e-11, "channel3": 2.5533860729586694e-10, "channel4": 4.76837158203125e-10}',
            b'{"version": 1, "message_type": "series-end", "message_id": 6, "detector_specific": {"read_overflow": false, "memory_overflow": false}}',
        ]

        numbegin = begin or 0
        numend = end or len(file_content)
        logging.info("trying to receive %d packets", numend - numbegin)
        task = asyncio.create_task(
            consume_zmq(
                context, numend - numbegin, typ=zmq.PULL, port=22004, return_data=True
            )
        )

        asyncio.create_task(
            stream_cbors(
                context,
                22004,
                PosixPath("tests/data/albaem-dump.cbors"),
                0.001,
                zmq.PUSH,
                begin=begin,
                end=end,
            )
        )

        pks = await task
        logging.info("data %s", pks)
        for i, pkg in enumerate(pks):
            assert pkg[0] == file_content[numbegin + i]


@pytest.mark.asyncio
async def test_cbor_empty(
    stream_cbors: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
        Coroutine[Any, Any, None],
    ],
    tmp_path: Any,
) -> None:
    with zmq.asyncio.Context() as context:
        tmpfile = tmp_path / "empty.cbor"

        with open(tmpfile, "wb") as f:
            f.write(b"")

        task = asyncio.create_task(
            consume_zmq(context, 1, typ=zmq.PULL, port=22004, return_data=True)
        )

        with pytest.raises(AssertionError):
            await stream_cbors(context, 22004, PosixPath(tmpfile), 0.001, zmq.PUSH)

        await cancel_and_wait(task)


@pytest.mark.asyncio
async def test_end_begin(
    stream_cbors: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int, int, int],
        Coroutine[Any, Any, None],
    ],
) -> None:
    with zmq.asyncio.Context() as context:
        with pytest.raises(AssertionError):
            await stream_cbors(
                context,
                22004,
                PosixPath("tests/data/albaem-dump.cbors"),
                0.001,
                zmq.PUSH,
                begin=5,
                end=3,
            )
