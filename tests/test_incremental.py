import numpy as np
import logging
from dranspose.helpers.incremental import IncrementalBuffer

logger = logging.getLogger(__name__)


def test_resize() -> None:
    n = 10
    data = np.arange(n)
    logger.debug(f"{data=}")
    buf = IncrementalBuffer(initial_rows=2, grow_factor=1.4, filler_value=-1)

    buf.add_entry(0, data[0])
    logger.debug(f"0, {buf._arr=}")
    assert buf._arr[1] == -1  # filler_value

    # add entry over capacity
    buf.add_entry(3, data[3])
    logger.debug(f"0, {buf._arr=}")
    assert buf[1] == -1  # test filler_value
    assert buf[2] == -1  # test filler_value
    assert buf._arr.shape[0] == 4  # test no oo loop, int(2*1.4) == 2

    # add entry over capacity
    buf.add_entry(5, data[5])
    logger.debug(f"0, {buf._arr=}")
    # test resized array being filled, int(int(4*1.3)*1.3) == 6
    assert buf._arr[6] == -1


def test_no_gaps() -> None:
    n = 10
    data = np.arange(n)
    logger.debug(f"{data=}")
    buf = IncrementalBuffer(
        initial_rows=2, grow_factor=2, filler_value=-1, no_gaps=True
    )

    assert not buf.has_entry(0)
    buf.add_entry(1, data[1])
    assert not buf.has_entry(0)
    buf.add_entry(0, data[0])
    assert buf.has_entry(0)

    for i in range(2, 6):
        assert not buf.has_entry(i)
        buf.add_entry(i, data[i])
        assert buf.has_entry(i)
    assert not buf.has_entry(7)
    buf.add_entry(7, data[7])
    assert buf.has_entry(7)
    logger.debug(f"{buf._arr=} {buf._seen=} {buf._next_missing=}")
    assert buf._seen == {7}
    assert buf._next_missing == 6

    buf.add_entry(8, data[8])
    assert buf.has_entry(8)
    logger.debug(f"{buf._arr=} {buf._seen=} {buf._next_missing=}")
    assert buf._seen == {7, 8}
    assert buf._next_missing == 6
    assert np.allclose(buf, [0, 1, 2, 3, 4, 5])  # test no_gaps

    assert not buf.has_entry(6)
    buf.add_entry(6, data[6])
    assert buf.has_entry(6)
    logger.debug(f"{buf._arr=} {buf._seen=} {buf._next_missing=}")
    assert len(buf._seen) == 0
    assert buf._next_missing == 9


def test_1d() -> None:
    rng = np.random.default_rng(2)
    n = 20
    data = rng.integers(0, 1000, size=(n,), dtype=np.int32)
    idx = np.arange(n)
    rng.shuffle(idx)
    logger.debug(f"{data=}")
    buf = IncrementalBuffer(initial_rows=2, filler_value=-1)
    assert buf.is_empty()
    for e, i in enumerate(idx):
        buf.add_entry(i, float(data[i]))
        logger.debug(f"{i=}, {buf._arr=}")
        logger.debug(f"{i=}, {sorted(buf._seen)=}")
        logger.debug(f"{i=}, {buf._next_missing=}")
        if e == 0:
            assert buf[0] == -1  # filler_value
    assert not buf.is_empty()
    logger.debug(buf.preview())
    out = np.asarray(buf)
    assert out.shape == data.shape
    assert np.allclose(out, data, atol=0)


def test_2d() -> None:
    rng = np.random.default_rng(3)
    rows, cols = 5, 6
    data = rng.integers(0, 256, size=(rows, cols)).astype(np.float32)
    logger.debug(f"{data=}")
    idx = np.arange(rows)
    rng.shuffle(idx)
    buf = IncrementalBuffer(initial_rows=2, grow_factor=1.3, filler_value=np.nan)
    for e, i in enumerate(idx):
        buf.add_entry(i, data[i])
        if e == 0:
            assert np.isnan(buf[0, 0])  # filler_value
        logger.debug(f"{i=}, {buf._arr=}")

    out = np.asarray(buf)
    assert buf.shape == data.shape
    assert out.shape == data.shape
    assert buf.dtype == data.dtype
    assert out.dtype == data.dtype
    assert np.allclose(out, data, atol=0)


def test_3d() -> None:
    rng = np.random.default_rng(3)
    rows, x_, y_ = 5, 2, 3
    data = np.arange(rows * x_ * y_, dtype=np.uint8).reshape(rows, x_, y_)
    logger.debug(f"{data=}")
    idx = np.arange(rows)
    rng.shuffle(idx)
    buf = IncrementalBuffer(initial_rows=2)
    for e, i in enumerate(idx):
        buf.add_entry(i, data[i])
        logger.debug(f"{i=}, {buf._arr=}")
        if e == 0:
            assert np.all(buf[0] == 0)  # filler_value
    out = np.asarray(buf)
    assert out.shape == data.shape
    assert out.dtype == data.dtype
    assert np.allclose(out, data, atol=0)
