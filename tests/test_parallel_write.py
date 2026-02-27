import asyncio
import logging
import os

# from pathlib import PosixPath
from typing import Awaitable, Callable, Any, Coroutine, Optional
import h5pyd
import h5py

# import numpy as np

import pytest
import zmq.asyncio
import zmq
from pydantic_core import Url
import aiohttp

from dranspose.ingester import Ingester

# from dranspose.ingesters.stins_parallel import (
#     StinsParallelIngester,
#     StinsParallelSettings,
# )
from dranspose.ingesters.stream1_parallel import Stream1ParallelIngester
from dranspose.ingesters.zmqpull_eiger_legacy import ZmqPullEigerLegacySettings

from dranspose.protocol import (
    StreamName,
    WorkerName,
    IngesterName,
    # WorkerTag,
    MappingName,
)

from dranspose.worker import Worker, WorkerSettings

from tests.utils import (
    wait_for_controller,
    wait_for_finish,
    set_sequence,
    # monopart_sequence,
    vworker,
    # set_uniform_sequence,
    uniform_sequence,
)


@pytest.mark.asyncio
async def test_writer(
    tmp_path: Any,
    controller: None,
    reducer: Callable[[Optional[str]], Awaitable[None]],
    create_worker: Callable[[Worker], Awaitable[Worker]],
    create_ingester: Callable[[Ingester], Awaitable[Ingester]],
    stream_eiger_dump: Callable[
        [
            zmq.Context[Any],
            os.PathLike[Any] | str,
            str,
            int,
        ],
        Coroutine[Any, Any, None],
    ],
) -> None:
    await reducer("examples.parallel_writer.reducer:WriterReducer")
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w1"),
                worker_class="examples.parallel_writer.worker:WriterWorker",
            ),
        )
    )
    await create_worker(
        Worker(
            settings=WorkerSettings(
                worker_name=WorkerName("w2"),
                worker_class="examples.parallel_writer.worker:WriterWorker",
            ),
        )
    )
    ing1 = await create_ingester(
        Stream1ParallelIngester(
            settings=ZmqPullEigerLegacySettings(
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
            ),
        )
    )
    ing2 = await create_ingester(
        Stream1ParallelIngester(
            settings=ZmqPullEigerLegacySettings(
                ingester_name=IngesterName("eiger-2"),
                ingester_streams=[StreamName("eiger")],
                upstream_url=Url("tcp://localhost:9999"),
                ingester_url=Url("tcp://localhost:10011"),
            ),
        )
    )

    await wait_for_controller(
        streams={StreamName("eiger")}, workers={WorkerName("w1"), WorkerName("w2")}
    )

    filename = tmp_path / "test.h5"
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "http://localhost:5000/api/v1/parameter/filename",
            data=f"{filename}",
        )
        assert resp.status == 200
        await resp.json()

    nmsg = 4
    seq = uniform_sequence(streams={StreamName("eiger")}, ntrig=nmsg)
    start_part = {"eiger": [[vworker()]]}
    seq["parts"]["start"] = start_part
    seq["sequence"].insert(0, MappingName("start"))
    logging.info("sequence %s", seq)
    await set_sequence(seq, all_wrap=False)

    with zmq.asyncio.Context() as context:
        asyncio.create_task(
            stream_eiger_dump(
                context,
                "tests/data/eiger_legacy_dump.zip",
                str(filename),
                9999,
                end=nmsg,
            )
        )
        content = await wait_for_finish()

        assert content == {
            "last_assigned": nmsg,
            "completed_events": nmsg,
            "total_events": nmsg,
            "finished": True,
        }

        logging.info("proc1 ev %d", ing1.state.processed_events)
        logging.info("proc2 ev %d", ing2.state.processed_events)
        assert ing1.state.processed_events < nmsg
        assert ing2.state.processed_events < nmsg
        assert ing1.state.processed_events + ing2.state.processed_events == nmsg

    def work() -> None:
        publish = h5pyd.File("http://localhost:5001/", "r")
        logging.info("workers: %s", list(publish["workers"].keys()))
        assert "w1" in publish["workers/w1/filename"][()].decode("utf-8")
        w_fname = publish["workers/w2/filename"][()].decode("utf-8")
        # w_frames = publish["frames/w2"][()].shape[0]

        with h5py.File(w_fname, "r") as f:
            # assert w_frames == f["/entry/instrument/eiger/data"].shape[0]
            assert f["/entry/instrument/eiger/data"].shape[1:] == (1065, 1030)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)
