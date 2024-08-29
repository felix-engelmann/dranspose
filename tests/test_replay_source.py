import json
from typing import Any

import pytest

from dranspose.replay import replay


@pytest.mark.asyncio
async def test_replay(
    tmp_path: Any,
) -> None:
    par_file = tmp_path / "parameters.json"

    with open(par_file, "w") as f:
        json.dump([{"name": "roi1", "data": "[10,20]"}], f)

    replay(
        "examples.dummy.worker:FluorescenceWorker",
        "examples.dummy.reducer:FluorescenceReducer",
        None,
        "examples.dummy.source:FluorescenceSource",
        par_file,
    )
