import asyncio
import logging
import threading
from typing import Any
import time

import h5pyd
import pytest
from readerwriterlock.rwlock import RWLockFair

from dranspose.replay import replay
from dranspose.event import ResultData
from dranspose.protocol import ParameterName, WorkParameter


logger = logging.getLogger(__name__)


block = threading.Event()
evt_published = threading.Event()


class BlockingReducer:
    def __init__(self, **kwargs: dict[str, object]) -> None:
        self.publish: dict[str, dict[int, int]] = {"map": {}}

        self.rw_lock = RWLockFair()
        self.publish_rlock = self.rw_lock.gen_rlock()
        self.publish_wlock = self.rw_lock.gen_wlock()

    def process_result(
        self,
        result: ResultData,
        parameters: dict[ParameterName, WorkParameter] | None = None,
    ) -> None:
        print(result)
        logger.info("processing_result %s", result)

        with self.publish_wlock:
            logger.info("writer lock acquired; waiting for block event")
            block.wait()
            logger.info("block event received")
            self.publish["map"][result.event_number] = 1
            logger.info("updated publish to %s", self.publish)
        time.sleep(0.5)
        evt_published.set()


class BlockingWorker:
    def __init__(self, **kwargs: dict[str, object]) -> None:
        pass

    def process_event(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return "not empty"


@pytest.mark.asyncio
@pytest.mark.do_not_fail_on_err_log
async def test_replay(
    tmp_path: Any,
) -> None:
    stop_event = threading.Event()
    done_event = threading.Event()

    thread = threading.Thread(
        target=replay,
        args=(
            "tests.test_reducer_lock:BlockingWorker",
            "tests.test_reducer_lock:BlockingReducer",
            None,
            "examples.dummy.source:FluorescenceSource",
            None,
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    block.clear()
    thread.start()
    time.sleep(1)

    def work() -> None:
        block.set()
        f = h5pyd.File("http://localhost:5010/", "r", timeout=1, retries=0)
        block.clear()
        time.sleep(1)
        with pytest.raises(KeyError):
            f["map"]
        block.set()
        logging.info("file %s", list(f.keys()))
        logging.info("map %s", list(f["map"].keys()))
        assert list(f.keys()) == ["map"]

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    done_event.wait()
    stop_event.set()

    thread.join()
