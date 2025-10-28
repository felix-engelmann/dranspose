# Getting Started - Analysis

This tutorial guides you to create a dranspose map-reduce analysis.

## Preparations

First create a folder (e.g. `drp-example`) and a python virtual environment, e.g. with venv or conda.

Install `dranspose` in it. Either use the release version or the main branch from maxiv internal or github.
Run one of the following.
```shell
pip install dranspose
pip install git+https://github.com/felix-engelmann/dranspose.git
pip install git+https://gitlab.maxiv.lu.se/scisw/daq-modules/dranspose.git
```

!!! info
    The maxiv git is the most up to date, the public github follows tightly. The official releases on pypi are only for bigger breaking changes.

To keep track of your changes, make the new folder a git repository with 
```shell
git init .
```

## Worker and Reducer

For a base analysis we need a worker function and a reducer function. Initially they are both empty skeletons.

It makes sense to separate the two required classes into separate files. The Worker class may be in `src/worker.py` and requires a `process_event` function as well as absorb optional arguments to `__init__`:
```python
# src/worker.py
class TestWorker:
    def __init__(self, *args, **kwargs):
        pass
    
    def process_event(self, event, *args, parameters=None, **kwargs):
        pass
```

The reducer is similar and only requires a `process_result` function. e.g. in `src/reducer.py`
```python
# src/reducer.py
class TestReducer:

    def __init__(self, *args, **kwargs):
        pass
    
    def process_result(self, result, parameters=None):
        pass
```

## Replaying captured data

With this in place, it is possible to replay captured data. Generally captures are provided by the staff from a generic experiment.
For this tutorial, you use a specifically crafted capture `dump_xrd.cbors` which needs to [downloaded](https://github.com/felix-engelmann/dranspose/tree/main/tests/data/dump_xrd.cbors) to a new `data` folder.

```shell
LOG_LEVEL="DEBUG" dranspose replay -w "src.worker:TestWorker" -r "src.reducer:TestReducer" -f data/dump_xrd.cbors
```

The `LOG_LEVEL="DEBUG"` is the most verbose output of the replay script. It does not output anything meaningful yet.

## Events

The first argument `event` to the worker function `process_event` is an EventData object.
The worker function needs to be able to handle all kind of events.

A first debugging step is to inspect the events and print them. A good way is to use the python logging system.
Create a logger at the top of the file and then print the event to debug:

```python
import logging

logger = logging.getLogger(__name__)

class TestWorker:

    def __init__(self, *args, **kwargs):
        pass

    def process_event(self, event, *args, parameters=None, **kwargs):
        logger.debug("event is %s", event)
```

The output then contains:

```
DEBUG:src.worker:event is event_number=0 streams={'xrd': StreamData(typ='STINS', frames=[b'{"htype": "header", "filename": "", "msg_number": 0}'], length=1)}
DEBUG:src.worker:event is event_number=1 streams={'xrd': StreamData(typ='STINS', frames=[b'{"htype": "image", "frame": 0, "shape": [10, 10], "type": "uint16", "compression": "none", "msg_number": 1}', b'\x05\x00\x12\x00\r\x00\x06\x00\x01\x00\x00\x00\x00\x00\x06\x00\x0e\x00\x13\x00\x11\x00\t\x00\x00\x00\x01\x00\x08\x00\x07\x00\x06\x00\x03\x00\x00\x00\n\x00\x0f\x00\x01\x00\x06\x00\x0f\x00\x13\x00\x14\x00\x12\x00\x10\x00\x05\x00\x00\x00\x06\x00\x02\x00\x10\x00\x13\x00\x13\x00\x12\x00\x12\x00\x14\x00\x0f\x00\x02\x00\x00\x00\x05\x00\x13\x00\x11\x00\x0b\x00\n\x00\x0c\x00\x10\x00\x13\x00\x05\x00\x00\x00\x08\x00\x14\x00\x0f\x00\x0c\x00\t\x00\t\x00\x0f\x00\x14\x00\n\x00\x00\x00\x08\x00\x13\x00\x13\x00\x0e\x00\x0b\x00\x0c\x00\x11\x00\x11\x00\x05\x00\x05\x00\x02\x00\x10\x00\x11\x00\x12\x00\x10\x00\x13\x00\x14\x00\x0f\x00\x03\x00\x0e\x00\x00\x00\x04\x00\r\x00\x14\x00\x13\x00\x14\x00\x0f\x00\x03\x00\x01\x00\x13\x00\x0b\x00\x01\x00\x02\x00\x08\x00\t\x00\x05\x00\x02\x00\x01\x00\x0c\x00'], length=2)}
```

That shows us the general structure of an [event][dranspose.event.EventData]. Every event has an `event_number` (the same event may be processed by multiple workers, so it is not unique).
The `streams` dictionary then contains all streams of data available in this event.
The values are [StreamData][dranspose.event.StreamData] objects with a `typ` and the raw zmq `frames`.

## Parse StreamData

As the raw zmq frames are not useful for further processing, dranspose provides parser for the default protocols.
For the `xrd` example, we need the STINS `parse` function from `dranspose.middlewares.stream1`.
If you import multiple parsers, it makes sense to rename them.

Then we check for the `xrd` stream, parse it and output it.

```python
import logging
from dranspose.middlewares.stream1 import parse as parse_stins

logger = logging.getLogger(__name__)

class TestWorker:

    def __init__(self, *args, **kwargs):
        pass

    def process_event(self, event, *args, parameters=None, **kwargs):
        if "xrd" in event.streams:
            acq = parse_stins(event.streams["xrd"])
            logger.debug("acquisition is %s", acq)
```

The debug output then shows the parsed `xrd` stream:

```
DEBUG:src.worker:data is msg_number=0 htype='header' filename=''
DEBUG:src.worker:data is msg_number=1 htype='image' frame=0 shape=[10, 10] type='uint16' compression='none' data=array([[ 5, 18, 13,  6,  1,  0,  0,  6, 14, 19],
       [17,  9,  0,  1,  8,  7,  6,  3,  0, 10],
       [15,  1,  6, 15, 19, 20, 18, 16,  5,  0],
       [ 6,  2, 16, 19, 19, 18, 18, 20, 15,  2],
       [ 0,  5, 19, 17, 11, 10, 12, 16, 19,  5],
       [ 0,  8, 20, 15, 12,  9,  9, 15, 20, 10],
       [ 0,  8, 19, 19, 14, 11, 12, 17, 17,  5],
       [ 5,  2, 16, 17, 18, 16, 19, 20, 15,  3],
       [14,  0,  4, 13, 20, 19, 20, 15,  3,  1],
       [19, 11,  1,  2,  8,  9,  5,  2,  1, 12]], dtype=uint16)
```

The STINS fields are parsed and the values are available as a numpy array in the `data` attribute.

## Useful Map function

The main goal of the worker (map function) is to map a large dataset to a smaller representation.
As an example analysis, we are only interested in the mean intensity in a region of interest in the top right corner.

The object returned by parse_stins depends on the position in the stream. [STINS][dranspose.data.stream1] defines a start, data and end messages.
Only the data messages contain a data attribute. We filter for them with the `isinstance`.

```python
import logging
import numpy as np

from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.data.stream1 import Stream1Data

logger = logging.getLogger(__name__)

class TestWorker:

    def __init__(self, *args, **kwargs):
        pass
        
    def process_event(self, event, *args, parameters=None, **kwargs):
        if "xrd" in event.streams:
            acq = parse_stins(event.streams["xrd"])
            if isinstance(acq, Stream1Data):
                intensity = np.mean(acq.data[:2,8:])
                logger.debug("intensity is %f", intensity)
                return intensity

```

Which outputs
``` 
DEBUG:src.worker:intensity is 9.500000
DEBUG:src.worker:intensity is 10.500000
DEBUG:src.worker:intensity is 11.000000
DEBUG:src.worker:intensity is 5.500000
```

We assume this mean value is the essence of the event which is also small enough to pass to the reduce step. 
Whatever the `process_event` event returns is passed to the reducer.

!!! warning
    Not all python objects can be returned. Especially functions cannot be passed to the reducer. All results are transferred via pickle. The replay script will throw an error if your object cannot be serialized.

## Reduce Function

The return values of the worker are passed to `process_result` function which we can output:

```python
import logging

logger = logging.getLogger(__name__)

class TestReducer:

    def __init__(self, *args, **kwargs):
        pass

    def process_result(self, result, parameters=None):
        logger.debug("result is %s", result)
```

This outputs

```
DEBUG:src.worker:intensity is 9.500000
DEBUG:src.reducer:result is event_number=1 worker='development0' parameters_hash='688787d8ff144c502c7f5cffaafe2cc588d86079f9de88304c26b0cb99ce91c6' payload=9.5
DEBUG:src.worker:intensity is 10.500000
DEBUG:src.reducer:result is event_number=2 worker='development0' parameters_hash='688787d8ff144c502c7f5cffaafe2cc588d86079f9de88304c26b0cb99ce91c6' payload=10.5
```

The [ResultData][dranspose.event.ResultData] has an `event_number` and a `payload`.
Note that for event 0, the payload is `None` as the worker did not return anything and therefore the reducer is not called.

!!! info
    Workers returning `None` on many events is a good strategy to cope with high frequency events to not overwhelm the reducer. 

## Inter Event Analysis

While the workers only have access to an event at a time, the reducer gets called for all events and can e.g. calculate temporal trends.
For that, a starting point is convenient. The `__init__` function is the convenient place.
We are interested in the delta of the intensity between each two events.

Opposite to initialising, the end of an experiment is useful to output the analysis a final time. `finish` is called once all results are processed.

The output of 
```python
import logging

logger = logging.getLogger(__name__)

class TestReducer:
    def __init__(self, *args, **kwargs):
        self.evolution = [0]
        self.last_value = 0

    def process_result(self, result, parameters=None):
        if result.event_number > (len(self.evolution) - 1):
            self.evolution += [None] * (1 + result.event_number - len(self.evolution))
        self.evolution[result.event_number] = result.payload - self.evolution
        self.last_value = result.payload.intensity
        
    def finish(self, parameters=None):
        logger.info("delta were %s", self.evolution)
```

is

```
INFO:src.reducer:delta were [0, 9.5, 1.0, 10.0, -4.5, 6.75, -3.75, 9.0, -1.5, 11.75]
```

The `process_result` function takes a lot of care on how to append data to the list.
This function may be called with different results, coming from different workers, for the *same* event.
Or a worker returns `None` for an event, so the `process_result` is never called for this event.
Due to these possibilities, there is no guarantee on the order of the events.
The code above takes care of this, by filling the list with `None` to the event received.

## Write results to file

Now that we have the delta values, it is nice to save them to a file.
This is best performed in the reducer.
To decide where to save the data, it is nice to get the path of the raw data first.
The STINS stream has a `filename` attribute in the series start message. 
By convention, it might be `""` (empty string) to indicate a live viewing without saving data.
Your pipeline should honor this as well and only save data if a filename is set.

The quickest way is for the worker to return a dictionary with either a `filename` key or an `intensity` key:

```python
import logging
import numpy as np

from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.data.stream1 import Stream1Data, Stream1Start, Stream1End

logger = logging.getLogger(__name__)

class TestWorker:

    def __init__(self, *args, **kwargs):
        pass

    def process_event(self, event, *args, parameters=None, **kwargs):
        logger.debug(event)
        if "xrd" in event.streams:
            acq = parse_stins(event.streams["xrd"])
            if isinstance(acq, Stream1Start):
                logger.info("start message %s", acq)
                return {"filename": acq.filename}
            elif isinstance(acq, Stream1Data):
                intensity = np.mean(acq.data[:2,8:])
                logger.debug("intensity is %f", intensity)
                return {"intensity": intensity}
```

The reducer then has to check which keys are present in the result and process them differently.

With only two keys, it is easy to keep an overview, however using dictionaries to pass data quickly becomes messy.
A cleaner solution is to return a dataclass depending on the intent.

```python
import logging
from dataclasses import dataclass
import numpy as np

from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.data.stream1 import Stream1Data, Stream1Start, Stream1End

logger = logging.getLogger(__name__)

@dataclass
class Start:
    filename: str

@dataclass
class Result:
    intensity: float

class TestWorker:

    def __init__(self, *args, **kwargs):
        pass

    def process_event(self, event, *args, parameters=None, **kwargs):
        logger.debug(event)
        if "xrd" in event.streams:
            acq = parse_stins(event.streams["xrd"])
            if isinstance(acq, Stream1Start):
                logger.info("start message %s", acq)
                return Start(acq.filename)
            elif isinstance(acq, Stream1Data):
                intensity = np.mean(acq.data[:2,8:])
                logger.debug("intensity is %f", intensity)
                return Result(intensity)
```

The reducer is then able to separate the different event types cleanly by importing the two dataclasses and checking with `isinstance`

```python
import logging
import h5py
import numpy as np

from .worker import Start, Result

logger = logging.getLogger(__name__)

class TestReducer:
    def __init__(self, *args, **kwargs):
        self.evolution = [0]
        self.last_value = 0

    def process_result(self, result, parameters=None):
        if isinstance(result.payload, Start):
            logger.info("start message")
        elif isinstance(result.payload, Result):
            logger.debug("got result %s", result.payload)
            if result.event_number > (len(self.evolution) - 1):
                self.evolution += [None] * (1 + result.event_number - len(self.evolution))
            self.evolution[result.event_number] = result.payload.intensity - self.last_value
            self.last_value = result.payload.intensity
```

This explicitly writes out the protocol on how the workers pass data to the reducer and avoids implicit contracts which are hard for others to understand and maintain.

Like in most trigger maps, the first packet of each stream is broadcast to all workers in the replay.
This makes sense as it is normally sent before the first trigger and contains only meta information.

This needs care when using the Start message in the reducer to open a file. The file should only be opened once, no matter how many worker messages it receives.
A simple way is to set a `None` file handle in `__init__` and only open the file if it is still `None`.

Before opening the file, the containing directory needs to be created with `os.makedirs`.
For this tutorial, the sample data has a filename of `output/xrd.h5` but it is usually an absolute path.
Beware that this is the filename where upstream the raw data is saved to. The analysis pipeline should write to a modified filename.

The reducer now saves the intensity values to a `_processed` suffixed file:

```python
import logging
import h5py
import os
import numpy as np

from .worker import Start, Result

logger = logging.getLogger(__name__)

class TestReducer:
    def __init__(self, *args, **kwargs):
        self.evolution = [0]
        self.last_value = 0
        self._fh = None
        self._dset = None

    def process_result(self, result, parameters=None):
        if isinstance(result.payload, Start):
            logger.info("start message")
            if self._fh is None:
                name, ext = os.path.splitext(result.payload.filename)
                dest_filename = f"{name}_processed{ext}"
                os.makedirs(os.path.dirname(dest_filename), exist_ok=True)
                self._fh = h5py.File(dest_filename, 'w')
                self._dset = self._fh.create_dataset("reduced", (0,), maxshape=(None, ), dtype=np.float32)
        elif isinstance(result.payload, Result):
            logger.debug("got result %s", result.payload)
            if result.event_number > (len(self.evolution) - 1):
                self.evolution += [None] * (1 + result.event_number - len(self.evolution))
            self.evolution[result.event_number] = result.payload.intensity - self.last_value
            self.last_value = result.payload.intensity

            oldsize = self._dset.shape[0]
            self._dset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._dset[result.event_number] = result.payload.intensity


    def finish(self, parameters=None):
        logger.info("delta were %s", self.evolution)
        if self._fh is not None:
            self._fh.close()
```

The file contains all the processed values

```
$ h5dump output/xrd_processed.h5
HDF5 "output/xrd_processed.h5" {
GROUP "/" {
   DATASET "reduced" {
      DATATYPE  H5T_IEEE_F32LE
      DATASPACE  SIMPLE { ( 10 ) / ( H5S_UNLIMITED ) }
      DATA {
      (0): 0, 9.25, 11.25, 11.75, 4.25, 2.25, 3, 4.75, 7.75, 10.75
      }
   }
}
}
```

## Processing Parameters

The current worker calculates the mean of a fixed area (`[:2,8:]`). Changing requires updating the code and pushing a new image, which is time-consuming.
To provide flexibility, dranspose supports parameters, which can change at any time.
Though, if you change them during an active scan, you get no guarantees on when which worker gets the new parameters.

To register parameters, the worker or reducer needs to implement a static `describe_parameters` method which returns a list of `dranspose.parameters` instances.
The parameter values, depending on their type, are available in the `parameters` dict and the `value` attribute:

```python
import logging
from dataclasses import dataclass
import numpy as np

from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.data.stream1 import Stream1Data, Stream1Start, Stream1End
from dranspose.parameters import IntParameter

logger = logging.getLogger(__name__)

@dataclass
class Start:
    filename: str

@dataclass
class Result:
    intensity: float

class TestWorker:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def describe_parameters():
        params = [
            IntParameter(name="till_x", default=2),
            IntParameter(name="from_y", default=8),
        ]
        return params

    def process_event(self, event, *args, parameters=None, **kwargs):
        logger.debug(event)
        if "xrd" in event.streams:
            acq = parse_stins(event.streams["xrd"])
            if isinstance(acq, Stream1Start):
                logger.info("start message %s", acq)
                return Start(acq.filename)
            elif isinstance(acq, Stream1Data):
                intensity = np.mean(acq.data[:parameters["till_x"].value,parameters["from_y"].value:])
                logger.debug("intensity is %f", intensity)
                return Result(intensity)
```

These parameters are managed by the dranspose controller and are get/set via the REST interface.
For replay testing, the parameters are provided in a json file with the value encoded as string in the `data` field:

```json
[{"name": "till_x", "data": "3"},
{"name": "from_y", "data": "7"}]
```

To provide the parameter file to the replay, add the `-p` argument

```shell
dranspose replay -w "src.worker:TestWorker" -r "src.reducer:TestReducer" -f data/dump_xrd.cbors -p test_params.json
```

As a good practice, it is recommended to store the parameters along with the processed data to be able to reconstruct how it was reduced.
The parameters defined in the worker are also available in the reducer:

```python
                self._fh = h5py.File(dest_filename, 'w')
                self._dset = self._fh.create_dataset("reduced", (0,), maxshape=(None, ), dtype=np.float32)
                self._fh.create_dataset("till_x", data=parameters["till_x"].value)
                self._fh.create_dataset("from_y", data=parameters["from_y"].value)
```

To result in a self descriptive file:

```
$ h5dump output/xrd_processed.h5
HDF5 "output/xrd_processed.h5" {
GROUP "/" {
   DATASET "from_y" {
      DATATYPE  H5T_STD_I64LE
      DATASPACE  SCALAR
      DATA {
      (0): 7
      }
   }
   DATASET "reduced" {
      DATATYPE  H5T_IEEE_F32LE
      DATASPACE  SIMPLE { ( 10 ) / ( H5S_UNLIMITED ) }
      DATA {
      (0): 0, 8.88889, 8.66667, 8.11111, 6, 6.66667, 8.22222, 10.6667,
      (8): 12.4444, 14.3333
      }
   }
   DATASET "till_x" {
      DATATYPE  H5T_STD_I64LE
      DATASPACE  SCALAR
      DATA {
      (0): 3
      }
   }
}
}
```

## Heavy Processing

For some applications the mean of a small rectangle might be sufficient, but more often it is interesting to analysse the full 2d image.
One option is to azimuthally integrate the image to get the intensity on a radial.
An approachable packet is the `azint` conda package.
After installing it, it needs the detector geometry. Initially, we will provide it fully manually by creating a `Detector` and a `Poni` instance.

After creating an `AzimuthalIntegrator` instance, we send the radial axis of *q* values to the reducer to save them to the h5 file and prepare the dimensions of the dataset with values, having the same length.

```python
import logging
from dataclasses import dataclass
import numpy as np
import azint

from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.data.stream1 import Stream1Data, Stream1Start, Stream1End
from dranspose.parameters import IntParameter

logger = logging.getLogger(__name__)

@dataclass
class Start:
    filename: str
    radial_axis: list[float]

@dataclass
class Result:
    intensity: float
    integrated: list[float]

class TestWorker:
    def __init__(self, *args, **kwargs):
        pixel_size = 0.75e-6
        det = azint.detector.Detector(pixel_size, pixel_size, (10,10))
        poni = azint.Poni(det,  dist=30*pixel_size,
                                poni1 = 5*pixel_size,
                                poni2 = 5*pixel_size,
                                rot1 = 0,
                                rot2 = 0,
                                rot3 = 0,
                                wavelength = 1e-10)
        self.ai = azint.AzimuthalIntegrator(poni,
                                 4,
                                 5,
                                 unit='q',
                                 solid_angle=True)

    @staticmethod
    def describe_parameters():
        params = [
            IntParameter(name="till_x", default=2),
            IntParameter(name="from_y", default=8),
        ]
        return params

    def process_event(self, event, *args, parameters=None, **kwargs):
        logger.debug(event)
        if "xrd" in event.streams:
            acq = parse_stins(event.streams["xrd"])
            if isinstance(acq, Stream1Start):
                logger.info("start message %s", acq)
                return Start(acq.filename, self.ai.radial_axis)
            elif isinstance(acq, Stream1Data):
                intensity = np.mean(acq.data[:parameters["till_x"].value,parameters["from_y"].value:])
                logger.debug("intensity is %f", intensity)
                I, _ = self.ai.integrate(acq.data)
                logger.info("I %s %s", I, self.ai.radial_axis)
                return Result(intensity, I)

```

### Poni file sources

There are multiple options on how to provide the geometry to build the poni file.
The individual float values could be exposed as parameters, however that will be inconvenient for users used to poni files.
There is a `BinaryParameter` available which holds the full content of a file. The advantage is that the workers don't need filesystem access to read the poni file.
However, another process needs to upload the poni file, which is normally the tango device server displaying the parameters.

## Writing the diffractograms

It remains to place the data calculated by the workers into the h5 file by the reducer.

```python
import logging
import h5py
import os
import numpy as np

from .worker import Start, Result

logger = logging.getLogger(__name__)

class TestReducer:
    def __init__(self, *args, **kwargs):
        self.evolution = [0]
        self.last_value = 0
        self._fh = None
        self._dset = None
        self._Iset = None

    def process_result(self, result, parameters=None):
        if isinstance(result.payload, Start):
            logger.info("start message")
            if self._fh is None:
                name, ext = os.path.splitext(result.payload.filename)
                dest_filename = f"{name}_processed{ext}"
                os.makedirs(os.path.dirname(dest_filename), exist_ok=True)
                self._fh = h5py.File(dest_filename, 'w')
                self._dset = self._fh.create_dataset("reduced", (0,), maxshape=(None, ), dtype=np.float32)
                self._fh.create_dataset("till_x", data=parameters["till_x"].value)
                self._fh.create_dataset("from_y", data=parameters["from_y"].value)
                number_qbins = len(result.payload.radial_axis)
                self._Iset = self._fh.create_dataset("I", (0, number_qbins), maxshape=(None, number_qbins), dtype=np.float32)
                self._fh.create_dataset("radial_axis", data=result.payload.radial_axis)
        elif isinstance(result.payload, Result):
            logger.debug("got result %s", result.payload)
            if result.event_number > (len(self.evolution) - 1):
                self.evolution += [None] * (1 + result.event_number - len(self.evolution))
            self.evolution[result.event_number] = result.payload.intensity - self.last_value
            self.last_value = result.payload.intensity

            oldsize = self._dset.shape[0]
            self._dset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._dset[result.event_number] = result.payload.intensity

            oldsize = self._Iset.shape[0]
            self._Iset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._Iset[result.event_number] = result.payload.integrated

    def finish(self, parameters=None):
        logger.info("delta were %s", self.evolution)
        if self._fh is not None:
            self._fh.close()
```
