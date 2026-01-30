import numpy as np
from typing import Optional, Union


class IncrementalBuffer:
    """
    Buffer incoming entries (scalars or arrays) that may arrive out-of-order.
    """

    def __init__(
        self,
        initial_rows: int = 16,
        grow_factor: float = 2,
        filler_value: Union[int, float] = 0,
        no_gaps: bool = False,
    ) -> None:
        """
        :param int initial_rows: Size of the initial biffer
        :param float grow_factor: Increased factor when the buffer needs to grow
        :param int|float filler_value: Default value for unreceived entries
        :param bool no_gaps: Return only contiguous entries when viewd as an array
        """
        assert isinstance(initial_rows, int)
        assert initial_rows >= 1
        self._capacity = initial_rows
        self._dtype: Optional[np.dtype] = None
        self._arr: Optional[np.ndarray] = None
        self._max_filled_index: int = -1
        self._filler_value = filler_value
        assert grow_factor > 1, "The grow factor must be > 1"
        self._grow_factor: float = grow_factor
        self._no_gaps = no_gaps
        self._seen = set()
        self._next_missing: int = 0

    def _init_buffer(self, first_entry: np.ndarray) -> None:
        self._dtype = first_entry.dtype
        self._arr = np.full(
            (self._capacity, *first_entry.shape), self._filler_value, dtype=self._dtype
        )

    def _ensure_capacity(self, req_size: int) -> None:
        if self._arr is None or self._dtype is None:
            raise RuntimeError("Buffer not initialized")
        if req_size <= self._arr.shape[0]:
            return
        new_shape = list(self._arr.shape)
        while new_shape[0] < req_size:
            new_shape[0] = max(int(new_shape[0] * self._grow_factor), new_shape[0] + 1)
            self._arr.resize(new_shape, refcheck=False)
            if self._filler_value != 0:
                self._arr[self._max_filled_index + 1 :] = self._filler_value
        # new_arr = np.full(new_shape, self._filler_value, dtype=self._dtype)
        # new_arr[: self._arr.shape[0]] = self._arr
        # self._arr = new_arr

    def add_entry(self, index: int, data: np._typing.ArrayLike) -> None:
        """
        Insert a scalar or array at the specified index (0-based).
        """
        if index >= self._next_missing:
            self._seen.add(index)
        while self._next_missing in self._seen:
            self._seen.remove(self._next_missing)
            self._next_missing += 1
        entry = np.array(data)
        if self._arr is None:
            self._init_buffer(entry)
        if entry.shape != self._arr.shape[1:]:
            raise ValueError("Incoming shape does not match buffer shape")

        if entry.dtype != self._dtype:
            entry = entry.astype(self._dtype, copy=False)

        self._ensure_capacity(
            index + 1
        )  # idx is 0 based -> to fit entry #3 the array needs to size 4
        self._arr[index] = entry
        if index > self._max_filled_index:
            self._max_filled_index = index

    def has_entry(self, index: int):
        return index < self._next_missing or index in self._seen

    def is_empty(self):
        return self._arr is None

    def preview(self, copy: bool = True, no_gaps: bool = False) -> np.ndarray:
        if self._arr is None:
            return np.zeros((0,))
        last = self._next_missing if no_gaps else self._max_filled_index + 1
        curr_view = self._arr[:last]
        return curr_view.copy() if copy else curr_view

    def __array__(self, dtype: Optional[np.dtype] = None) -> np.ndarray:
        out = self.preview(copy=False, no_gaps=self._no_gaps)
        if dtype is not None:
            return out.astype(dtype, copy=False)
        return out

    def __getitem__(self, key):
        view = self.preview(copy=False)
        return view[key]

    @property
    def shape(self) -> tuple:
        view = self.preview(copy=False)
        return view.shape

    @property
    def dtype(self) -> np.dtype:
        if self._dtype is None:
            raise ValueError("Buffer not initialized")
        return self._dtype
