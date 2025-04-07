import asyncio
import logging

from dranspose.helpers.utils import cancel_and_wait
from dranspose.protocol import (
    WorkerName,
)

import pytest

from dranspose.worker import Worker, WorkerSettings
from tests.utils import wait_for_controller


@pytest.mark.asyncio
async def test_context(
    controller: None,
) -> None:
    worker = Worker(
        settings=WorkerSettings(
            worker_name=WorkerName("w5555"),
            worker_class="examples.repub.worker:RepubWorker",
        ),
    )
    worker_task = asyncio.create_task(worker.run())

    await wait_for_controller(workers={WorkerName("w5555")})

    logging.info("custom context is %s", worker.custom_context)
    assert worker.custom_context["context"].closed is False
    await worker.close()
    await cancel_and_wait(worker_task)
    logging.info("custom context is %s", worker.custom_context)
    assert worker.custom_context["context"].closed is True
