# Developing an Analysis from HDF5 Data

Sometimes, recording the data to develop an analysis in advance is not feasible.
The replay module of dranspose usually reads from recorded data via cbors files.

However, it is possible to provide custom streams instead of cbors.
Special attention needs to be provided to create a stream which is similar to the one encounted at the experiment.

Instead of providing the `-f` parameter to replay, use `--source` or `-s` to provide a python class path, e.g.:

    dranspose replay -w "src.worker:FluorescenceWorker" -r "src.reducer:FluorescenceReducer" -s "src.hdf5_sources:FluorescenceSource" -p params.json

The `FluorescenceSource` class only needs to implement one function: `get_source_generators` which has to return a list of generators.

The generators themselves may be methods of this class, but can be implemented anywhere.
The generator is supposed to yield an [dranspose.event.InternalWorkerMessage][] object.

## Example with STINS

To stream 2d images from a hybrid photon counting detector, the following source class could be adapted.
The images are read from a HDF5 file.

```python
import itertools

from dranspose.event import (
    InternalWorkerMessage,
    StreamData
)
from dranspose.data.stream1 import Stream1Start, Stream1Data, Stream1End
import h5py
from bitshuffle import compress_lz4

class FluorescenceSource:
    def __init__(self):
        self.fd = h5py.File("../000008.h5")
    
    def get_source_generators(self):
        return [self.pilatus_source()]

    def pilatus_source(self):
        msg_number = itertools.count(0)

        stins_start = Stream1Start(htype="header", filename="", msg_number=next(msg_number)).model_dump_json()
        start = InternalWorkerMessage(event_number=0, streams={"pilatus": StreamData(typ="STINS", frames=[stins_start])})
        yield start

        frameno = 0
        for image in self.fd["/entry/measurement/pilatus/frames"]:
            stins = Stream1Data(htype="image", msg_number=next(msg_number),
                                frame=frameno, shape=image.shape, compression="bslz4",
                                type=str(image.dtype)).model_dump_json()
            dat = compress_lz4(image)
            img = InternalWorkerMessage(event_number=frameno+1,
                                          streams={"pilatus": StreamData(typ="STINS", frames=[stins, dat.tobytes()])})
            yield img
            frameno += 1

        stins_end = Stream1End(htype="series_end", msg_number=next(msg_number)).model_dump_json()
        end = InternalWorkerMessage(event_number=frameno,
                                      streams={"pilatus": StreamData(typ="STINS", frames=[stins_end])})
        yield end
```

The `__init__` only opens the `.h5` file.
The mandatory method `get_source_generators` returns a list with a single generator.
This generator `pilatus_source` then creates an [dranspose.data.stream1.Stream1Start][] object and dumps it to json.
The bytes resulting from that dump to json are then first wrapped in [dranspose.event.StreamData][].
Here we used the stream name `pilatus`, which needs to match the setup at the experiment.
Finally, it yields an InternalWorkerMessage.

For the data packages, the actual data needs to be returned. For the header we use [dranspose.data.stream1.Stream1Data][] again and dump it to json.
For the image, it is important to know if the stream will contain compressed images.
In the above case, the stream will send bitshuffle lz4 compressed images.
Therefore, we need to recreate the compressed frames with `compress_lz4`.

!!! warning
    Beware that you have to manually provide the correct `event_number` for each yield.

In STINS, there is usually a [dranspose.data.stream1.Stream1End][] message, which needs to be yielded.

