import asyncio
import json
import logging
import threading
from typing import Any

import aiohttp
import pytest
import h5pyd

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
            "examples.params.worker:ParamWorker",
            "examples.dynamic_reduce.reducer:InteractiveReducer",
            None,
            "examples.dynamic_reduce.source:SlowSource",
            par_file,
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    thread.start()
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await session.get("http://localhost:5010/api/status/")
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                pass
        await asyncio.sleep(1)

        f = h5pyd.File("http://localhost:5010/", "r")
        logging.info(
            f"file {list(f.keys())}",
        )
        logging.warning("int_param %s", f["params"]["int_param"])
        assert f["params"]["int_param"][()] == 0

        resp = await session.post(
            "http://localhost:5010/api/v1/parameter/int_param",
            data=b"42",
        )
        assert resp.status == 200

        await asyncio.sleep(3)

        f = h5pyd.File("http://localhost:5010/", "r")
        logging.info(
            f"file {list(f.keys())}",
        )
        logging.warning("int_param %s", f["params"]["int_param"])
        assert f["params"]["int_param"][()] == 42

    done_event.wait()

    stop_event.set()

    thread.join()
