import datetime
import itertools

import numpy as np

from dranspose.data.contrast import ContrastStarted, ContrastRunning, ContrastFinished
from dranspose.data.positioncap import (
    PositionCapStart,
    PositionCapField,
    PositionCapValues,
    PositionCapEnd,
)
from dranspose.data.xspress3 import XspressStart, XspressImage, XspressEnd
from dranspose.event import InternalWorkerMessage

from dranspose.data.stream1 import Stream1Start, Stream1Data, Stream1End

# import h5py
# from bitshuffle import compress_lz4


class FluorescenceSource:
    def __init__(self):
        # self.fd = h5py.File("../nanomax/000008.h5")
        pass

    def get_source_generators(self):
        return [
            self.pilatus_source(),
            self.contrast_source(),
            self.xspress3_source(),
            self.pcap_source(),
        ]

    def pilatus_source(self):
        msg_number = itertools.count(0)

        stins_start = Stream1Start(filename="", msg_number=next(msg_number))
        start = InternalWorkerMessage(
            event_number=0,
            streams={"pilatus": stins_start.to_stream_data()},
        )
        yield start

        frameno = 0
        # for image in self.fd["/entry/measurement/pilatus/frames"]:
        for _ in range(10):
            image = np.ones((10, 10))
            # dat = compress_lz4(image)
            stins = Stream1Data(
                msg_number=next(msg_number),
                frame=frameno,
                shape=image.shape,
                compression="none",  # compression="bslz4"
                type=str(image.dtype),
                data=image,
            )
            img = InternalWorkerMessage(
                event_number=frameno + 1,
                streams={"pilatus": stins.to_stream_data()},
            )
            yield img
            frameno += 1

        stins_end = Stream1End(msg_number=next(msg_number))
        end = InternalWorkerMessage(
            event_number=frameno + 1,
            streams={"pilatus": stins_end.to_stream_data()},
        )
        yield end

    def contrast_source(self):
        start = ContrastStarted(path="", scannr=4, description="stepscan")
        yield InternalWorkerMessage(
            event_number=0,
            streams={"contrast": start.to_stream_data()},
        )

        data = {}
        data["x"] = np.ones((10,))  # self.fd["entry/measurement/pseudo/x"][:]
        data["y"] = np.ones((10,))  # self.fd["entry/measurement/pseudo/y"][:]
        data["z"] = np.ones((10,))  # self.fd["entry/measurement/pseudo/z"][:]

        for i in range(len(data["x"])):
            pseudo = {k: np.array([v[i]]) for k, v in data.items()}
            print(pseudo)
            pos = ContrastRunning(pseudo=pseudo, dt=0.4)
            yield InternalWorkerMessage(
                event_number=i + 1,
                streams={"contrast": pos.to_stream_data()},
            )

        end = ContrastFinished(path="", scannr=4, description="stepscan")
        yield InternalWorkerMessage(
            event_number=11,
            streams={"contrast": end.to_stream_data()},
        )

    def xspress3_source(self):
        start = XspressStart(filename="")
        yield InternalWorkerMessage(
            event_number=0,
            streams={"xspress3": start.to_stream_data()},
        )

        frameno = 0
        # for spec in self.fd["/entry/measurement/xspress3/data"]:
        for _ in range(10):
            spec = np.ones((4, 10))
            dat = XspressImage(
                frame=frameno,
                shape=spec.shape,
                exptime=0.099999875,
                type=str(spec.dtype),
                compression="none",
                data=spec,
                meta={"ocr": 1},
            )
            yield InternalWorkerMessage(
                event_number=frameno + 1,
                streams={"xspress3": dat.to_stream_data()},
            )
            frameno += 1

        end = XspressEnd()
        yield InternalWorkerMessage(
            event_number=frameno + 1,
            streams={"xspress3": end.to_stream_data()},
        )

    def pcap_source(self):
        fields = [
            PositionCapField(name="PCAP.BITS0.Value", type="uint32"),
            PositionCapField(name="INENC1.VAL.Mean", type="double"),
            PositionCapField(name="PCAP.TS_TRIG.Value", type="double"),
        ]
        start = PositionCapStart(arm_time=datetime.datetime.utcnow())
        yield InternalWorkerMessage(
            event_number=0,
            streams={"pcap": start.to_stream_data(fields)},
        )

        data = [
            np.ones((10,), dtype=np.uint32),
            np.ones(
                (10,), dtype=np.float64
            ),  # self.fd["/entry/measurement/panda0/INENC1.VAL_Mean"][:],
            np.ones(
                (10,), dtype=np.float64
            ),  # self.fd["/entry/measurement/panda0/PCAP.TS_TRIG_Value"][:]
        ]

        for i in range(len(data[0])):
            for f, d in zip(fields, data):
                f.value = d[i]

            print(fields)
            val = PositionCapValues(fields={f.name: f for f in fields})
            yield InternalWorkerMessage(
                event_number=i + 1,
                streams={"pcap": val.to_stream_data()},
            )

        end = PositionCapEnd()
        yield InternalWorkerMessage(
            event_number=11,
            streams={"pcap": end.to_stream_data()},
        )
