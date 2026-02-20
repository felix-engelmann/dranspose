import asyncio
import logging
import os
from pathlib import PosixPath
from typing import Awaitable, Callable, Any, Coroutine, Optional
import h5pyd

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
    stream_cbors: Callable[
        [zmq.Context[Any], int, os.PathLike[Any] | str, float, int],
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

    ntrig = 4
    seq = uniform_sequence(streams={StreamName("eiger")}, ntrig=ntrig)
    start_part = {"eiger": [[vworker()]]}
    seq["parts"]["start"] = start_part
    seq["sequence"].insert(0, MappingName("start"))
    logging.info("sequence %s", seq)
    await set_sequence(seq, all_wrap=False)

    with zmq.asyncio.Context() as context:
        asyncio.create_task(
            stream_cbors(
                context,
                9999,
                PosixPath("tests/data/eiger-small.cbors"),
                0.1,
                zmq.PUSH,
                begin=0,
            )
        )

        content = await wait_for_finish()

        assert content == {
            "last_assigned": ntrig,
            "completed_events": ntrig,
            "total_events": ntrig,
            "finished": True,
        }

        logging.info("proc1 ev %d", ing1.state.processed_events)
        logging.info("proc2 ev %d", ing2.state.processed_events)
        assert ing1.state.processed_events < ntrig
        assert ing2.state.processed_events < ntrig
        assert ing1.state.processed_events + ing2.state.processed_events == ntrig

    def work() -> None:
        publish = h5pyd.File("http://localhost:5001/", "r")
        logging.info("workers: %s", list(publish["workers"].keys()))
        assert "w1" in publish["workers/w1/filename"][()].decode("utf-8")
        assert "w2" in publish["workers/w2/filename"][()].decode("utf-8")
        # The last message is lost with the parallel ingester for Stream1
        # assert f[f"results/{ntrig}/eiger/htype"][()] == b"series_end"
        # for i in range(1, ntrig):
        #     # assert f[f"results/{i}/eiger/msg_number"][()] == i
        #     assert publish[f"results/{i}/eiger/htype"][()] == b"dimage-1.0"
        #     assert publish[f"results/{i}/eiger/frame"][()] == i - 1

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)
