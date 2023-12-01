# Application Development

Unlike sequential processing of data, dranspose leverages parallel processing to achieve the throughput necesary for live processing.

We use the *map reduce* programming model for distributing work.
Parallel workers unfortunately have to work independently. Therefore, they only have access 
to a single event at a time. They may store local state, but don't have access to arbitrary other events.

For data analysis which has to cross all events, there is a secondary *reduce* step which can only cope with reduced data, but gets all events delivered.

A lot of common analysis tasks are easily mapped to this programming model.
The map phase perform the heavy lifting, e.g. analysing images and then forwards a spectrum to the reducer which only averages the sprectra or appends them to a list.

## Using `dranspose`

!!! note
    Before developing a new worker functionality, it is necessary to capture events coming from the streams.
    Please see [capturing events](../deployment/capturing.md).
    Here we assume that you have dumps for all necessary streams


To analyse data with dranspose, you need to split your task into a `map` and a `redude` function.

Create a new git repository and create the following structure
    
    .
    ├── Dockerfile
    ├── README.md
    ├── requirements.txt
    └── src
        ├── reducer.py
        └── worker.py
   

### `worker.py`

The worker gets events and has to first parse the messages from the data stream.
Then it reduces the data available and forwards a condensed version.

The worker class is instantiated for every new trigger map. This allows to use the `__init__` for resetting the state.
All calls provide the current set of `parameters` which may be required to set the worker up.

Creating a logger is useful for development and production.

```python
import logging

from dranspose.event import EventData
from dranspose.middlewares import contrast
from dranspose.middlewares import xspress

logger = logging.getLogger(__name__)

class FluorescenceWorker:
    def __init__(self, parameters=None):
        self.number = 0

```
The `process_event` function gets an [EventData](../reference/protocols/events.md) object which contains all required streams for the current event.
The first step should be to check that the required streams for the analyis are present.
```python
    def process_event(self, event: EventData, parameters=None):
        logger.debug("using parameters %s", parameters)
        if {"contrast", "xspress3"} - set(event.streams.keys()) != set():
            logger.error(
                "missing streams for this worker, only present %s", event.streams.keys()
            )
            return
```
Many streams have the same packet structure and therefore `dranspose` include [middlewares](../reference/middlewares.md) to autmatically parse the frames to python objects.
```python
        try:
            con = contrast.parse(event.streams["contrast"])
        except Exception as e:
            logger.error("failed to parse contrast %s", e.__repr__())
            return
    
        try:
            spec = xspress.parse(event.streams["xspress3"])
        except Exception as e:
            logger.error("failed to parse xspress3 %s", e.__repr__())
            return
        logger.error("contrast: %s", con)
        logger.error("spectrum: %s", spec)
```
Check if we are in an event which produces data. Others may be `starting` or `finishing`
```python
        if con.status == "running":
            # new data
            sx, sy = con.pseudo["x"][0], con.pseudo["y"][0]
            logger.error("process position %s %s", sx, sy)

            roi1 = spec.data[3][parameters["roi1"][0] : parameters["roi1"][1]].sum()

            return {"position": (sx, sy), "concentations": {"roi1": roi1}}
```

The returned object will be available to the reducer

### `reducer.py`

The reducer may also have a setup in `__init__` where is may initialise the special `publish` attribute.
This attribute is automatically exposed via *http* for live viewers

```python
from dranspose.event import ResultData

class FluorescenceReducer:
    def __init__(self, parameters=None):
        self.number = 0
        self.publish = {"map": {}}
```
In `process_result`, only simple operations are possible, such as appending to a dictionary, as this has to run at the acquisition speed.
```python
    def process_result(self, result: ResultData, parameters=None):
        print(result)
        if result.payload:
            self.publish["map"][result.payload["position"]] = result.payload[
                "concentations"
            ]
```

## Developing an analysis

The worker and reducer is easily tested by the `dranspose replay` command.
Provide the worker class `-w` and the reducer class `-r`.
You also need to provide the dumped stream from each ingester.
If you need parameters, you can provide a json or pickle file which will be provided to your worker functions.

```shell
LOG_LEVEL="DEBUG" dranspose replay -w "src.worker:FluorescenceWorker" \
    -r "src.reducer:FluorescenceReducer" \
    -f ../contrast_ingest.pkls ../xspress_ingest.pkls \
    -p ../fullparam.json
```

