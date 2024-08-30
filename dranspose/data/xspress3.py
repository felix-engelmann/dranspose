import pickle
from typing import Literal, Optional

import zmq
from pydantic import BaseModel, TypeAdapter, ConfigDict

from dranspose.event import StreamData


class XspressBase(BaseModel):
    def to_stream_data(self) -> StreamData:
        js = self.model_dump_json(exclude={"data", "meta"}).encode()
        frames = [zmq.Frame(js)]
        if hasattr(self, "data"):
            frames.append(zmq.Frame(self.data.tobytes()))
        if hasattr(self, "meta"):
            pk = pickle.dumps(list(self.meta.values()))
            frames.append(zmq.Frame(pk))
        return StreamData(typ="xspress", frames=frames)


class XspressStart(XspressBase):
    """
    Example:
        ``` py
        XspressStart(
            htype='header',
            filename='/data/.../diff_1130_stream_test/raw/dummy/scan_000002_xspress3.hdf5',
            overwritable=False
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["header"] = "header"
    filename: str


class XspressImage(XspressBase):
    """
    While the original stream sends 3 separate zmq frames (no multipart), this returns a single packet.

    Example:
        ``` py
        XspressImage(
            htype='image',
            frame=0,
            shape=[4, 4096],
            exptime=0.099999875,
            type='uint32',
            compression='none',
            data=array([[0, 0, 0, ..., 0, 0, 0],
               [0, 0, 0, ..., 0, 0, 0],
               [0, 0, 0, ..., 0, 0, 0],
               [0, 0, 0, ..., 0, 0, 0]], dtype=uint32),
            meta={
                'ocr': array([0.00000000e+00, 0.00000000e+00, 0.00000000e+00, 1.56250195e-15]),
                'AllEvents': array([2, 0, 0, 3], dtype=uint32),
                'AllGood': array([0, 0, 0, 1], dtype=uint32),
                'ClockTicks': array([7999990, 7999990, 7999990, 7999990], dtype=uint32),
                'TotalTicks': array([7999990, 7999990, 7999990, 7999990], dtype=uint32),
                'ResetTicks': array([ 0,  0,  0, 91], dtype=uint32),
                'event_widths': array([6, 6, 6, 6], dtype=int32),
                'dtc': array([1.00000175, 1.        , 1.        , 1.000014  ])
            }
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["image"] = "image"
    frame: int
    shape: list[int]
    exptime: Optional[float] = 1
    type: str
    compression: Optional[str] = "none"


class XspressEnd(XspressBase):
    """
    Example:
        ``` py
        XspressEnd(htype='series_end')
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["series_end"] = "series_end"


XspressPacket = TypeAdapter(XspressStart | XspressImage | XspressEnd)
"""
Union type for Xspress packets
"""
