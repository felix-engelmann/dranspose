# K8s deployment

The most easy way to run a distributed version of dranspose is via a helm chart.


## Values

The required values are a `beamline` to be able to mount the correct volumes.
The `dump_prefix` may be set to a path to which the ingesters dump all stream messages.
This is useful to get the initial data to develop a worker which can digest these.

The `ingesters` map specifies the name of the stream, the `connect_url` on where to connect to and the class.
Other ingesters may need additional settings.

The `workers` and the `reducer` run with a custom docker image which contains all the dependencies for the analysis
`worker.class` and `reducer.class` specify the paths to the correct classes in the analysis container. 

```yaml
global:
  beamline: nanomax
  dump_prefix: false #"/data/staff/dummymax/dumps/ingest_dump_"

ingesters:
  contrast:
    connect_url: "tcp://172.16.125.30:5556"
    ingester_class: "StreamingContrastIngester"
  xspress3:
    connect_url: "tcp://172.16.126.70:9999"
    ingester_class: "StreamingXspressIngester"

science_image: "harbor.maxiv.lu.se/daq/dranspose/nanomax-fluorescence:main"

worker:
  class: "src.worker:FluorescenceWorker"
reducer:
  class: "src.worker:FluorescenceReducer"
```
