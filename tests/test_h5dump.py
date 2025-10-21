from contextlib import nullcontext
from typing import Any, ContextManager, Tuple

import h5py
import numpy as np
import pytest
from dranspose.helpers.h5dump import dump


@pytest.mark.asyncio
async def test_dump(
    tmp_path: Any,
) -> None:
    def get_data() -> Tuple[dict[str, Any], ContextManager[None]]:
        dt = np.dtype({"names": ["a", "b"], "formats": [float, int]})

        arr = np.array([(0.5, 1)], dtype=dt)

        data = {
            "live": 34,
            "other": {"third": [1, 2, 3]},  # only _attrs allowed in root
            "other_attrs": {"NX_class": "NXother"},
            "spaced group": {"space ds": 4, "space ds_attrs": {"bla": 5}},
            "spaced group_attrs": {"spaced attr": 3},
            "image": np.ones((1000, 1000)),
            "specialtyp": np.ones((10, 10), dtype=">u8"),
            "specialtyp_attrs": {"NXdata": "NXspecial"},
            "composite": arr,
            "composite_attrs": {"axes": ["I", "q"]},
            "nested grp": {"second level": {"third": {"still dict": {"value": 1}}}},
            "hello": "World",
            "_attrs": {"NX_class": "NXentry"},
        }
        return data, nullcontext()

    dump_file = tmp_path / "test.h5"

    d, lock = get_data()
    dump(d, dump_file, lock)

    res = h5py.File(dump_file)

    assert res["live"][()] == 34
    assert res["nested grp/second level/third/still dict/value"][()] == 1
    assert res["spaced group/space ds"].attrs["bla"] == 5
    assert res.attrs["NX_class"] == "NXentry"
