import asyncio
import json
import logging
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

    rep = replay(
        "examples.dummy.worker:FluorescenceWorker",
        "examples.dummy.reducer:FluorescenceReducer",
        None,
        "examples.dummy.source:FluorescenceSource",
        par_file,
        port=5010,
    )

    evt = next(rep)
    next(rep)

    def work():
        f = h5pyd.File("/", "r", endpoint="http://localhost:5010/data")
        logging.info("file %s", list(f.keys()))
        logging.info("map %s", list(f["map"].keys()))
        assert list(f.keys()) == ["map"]

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, work)

    evt.set()
    try:
        next(rep)
    except StopIteration:
        pass
