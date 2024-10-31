import itertools
import time

import numpy as np

from dranspose.event import InternalWorkerMessage

from dranspose.data.stream1 import Stream1Start, Stream1Data, Stream1End


class SlowSource:
    def __init__(self):
        pass

    def get_source_generators(self):
        return [
            self.pilatus_source(),
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
            time.sleep(1)

        stins_end = Stream1End(msg_number=next(msg_number))
        end = InternalWorkerMessage(
            event_number=frameno + 1,
            streams={"pilatus": stins_end.to_stream_data()},
        )
        yield end
