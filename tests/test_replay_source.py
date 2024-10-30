import asyncio
import json
import logging
import threading
from typing import Any

import h5pyd
import pytest

from dranspose.replay import replay


@pytest.mark.asyncio
async def test_replay(
    tmp_path: Any,
) -> None:
    par_file = tmp_path / "parameters.json"

    with open(par_file, "w") as f:
        json.dump([{"name": "roi1", "data": "[10,20]"}], f)

    stop_event = threading.Event()
    done_event = threading.Event()

    thread = threading.Thread(
        target=replay,
        args=(
            "examples.dummy.worker:FluorescenceWorker",
            "examples.dummy.reducer:FluorescenceReducer",
            None,
            "examples.dummy.source:FluorescenceSource",
            par_file,
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    thread.start()

    done_event.wait()

    def work() -> None:
        f = h5pyd.File("http://localhost:5010/", "r")
        logging.info("file %s", list(f.keys()))
        logging.info("map %s", list(f["map"].keys()))
        assert list(f.keys()) == ["map"]

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    stop_event.set()

    thread.join()
