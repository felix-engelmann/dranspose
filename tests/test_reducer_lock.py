import asyncio
import logging
import threading
from typing import Any
from time import sleep

import h5pyd
import pytest
from readerwriterlock.rwlock import RWLockFair

from dranspose.replay import replay
from dranspose.event import ResultData
from dranspose.protocol import ParameterName, WorkParameter


logger = logging.getLogger(__name__)


allow_lock_release = threading.Event()
lock_acquired = threading.Event()
allow_processing = threading.Event()


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
        logger.info("ready to process result %s", result)
        allow_processing.wait()
        with self.publish_wlock:
            lock_acquired.set()
            logger.info("writer lock acquired")
            self.publish["map"][result.event_number] = 1
            logger.info("updated publish to %s", self.publish)
            allow_lock_release.wait()
        lock_acquired.clear()
        logger.info("processing done")


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
@pytest.mark.allow_errors_in_log
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
    thread.start()
    sleep(1)

    def work() -> None:
        logger.info("opening remote file")
        f = h5pyd.File("http://localhost:5010/", "r", timeout=1, retries=0)
        logger.info("remote file opened")
        allow_processing.set()
        logger.info("allow process_result to continue")
        sleep(0.1)
        for i in range(5):
            logger.info(f"iteration {i}")
            allow_lock_release.clear()
            logger.info("allow_lock_release cleared, wait for red to acq lock")
            lock_acquired.wait()
            logger.info("try to read data")
            with pytest.raises(KeyError):
                f["map"]
                logging.warning("map %s (this should never appear)", list(f["map"]))
            logger.info("read should have failed, allow reducer to terminate")
            allow_lock_release.set()
        sleep(1)
        logging.info("file %s", list(f.keys()))
        logging.info("map %s", list(f["map"].keys()))
        assert list(f.keys()) == ["map"]

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    done_event.wait()
    stop_event.set()

    thread.join()
