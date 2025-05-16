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


process_result_called = threading.Event()
lock_acquired = threading.Event()
file_opened = threading.Event()
test_done = threading.Event()


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
        process_result_called.set()
        file_opened.wait()
        process_result_called.clear()
        with self.publish_wlock:
            lock_acquired.set()
            self.publish["map"][str(result.event_number)] = 1

            test_done.wait()
            lock_acquired.clear()


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

    def work() -> None:
        f = None
        for i in range(5):
            process_result_called.wait()
            test_done.clear()
            if f is None:
                f = h5pyd.File("http://localhost:5010/", "r", timeout=1, retries=0)
            file_opened.set()
            lock_acquired.wait()
            file_opened.clear()
            with pytest.raises(KeyError):
                f["map"]
            test_done.set()
        file_opened.set()
        sleep(1)
        assert f is not None
        logging.info("file %s", list(f.keys()))
        logging.info("map %s", list(f["map"].keys()))
        assert list(f.keys()) == ["map"]

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    done_event.wait()
    stop_event.set()

    thread.join()
