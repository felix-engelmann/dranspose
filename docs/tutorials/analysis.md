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
    
    def process_event(self, event, parameters=None):
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
    def process_event(self, event, parameters=None):
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
    def process_event(self, event, parameters=None):
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
    def process_event(self, event, parameters=None):
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

    def process_result(self, result, parameters=None):
        self.evolution.append(result.payload-self.evolution[-1])

    def finish(self, parameters=None):
        logger.info("delta were %s", self.evolution)
```

is

```
INFO:src.reducer:delta were [0, 9.5, 1.0, 10.0, -4.5, 6.75, -3.75, 9.0, -1.5, 11.75]
```