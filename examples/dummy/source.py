import itertools

import numpy as np

from dranspose.event import InternalWorkerMessage

from dranspose.data.stream1 import Stream1Start, Stream1Data, Stream1End

# import h5py
# from bitshuffle import compress_lz4


class FluorescenceSource:
    def __init__(self):
        # self.fd = h5py.File("../000008.h5")
        pass

    def get_source_generators(self):
        return [self.pilatus_source()]

    def pilatus_source(self):
        msg_number = itertools.count(0)

        stins_start = Stream1Start(
            htype="header", filename="", msg_number=next(msg_number)
        )
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
                htype="image",
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

        stins_end = Stream1End(htype="series_end", msg_number=next(msg_number))
        end = InternalWorkerMessage(
            event_number=0,
            streams={"pilatus": stins_end.to_stream_data()},
        )
        yield end
