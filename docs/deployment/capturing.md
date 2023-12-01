# Capturing Streams

To develop locally, there are 2 possible streams to capture.
The raw stream from the device/republishing is useful to develop ingesters for new protocols.
The InternalWorkerMessages are already processed by the ingesters and are more useful to develop the worker and reducer code.

## Raw Streams

Capturing the raw streams is the first step for a new setup.
Figure out a minimal script to listen to the data stream and write the received data to disk.
It has to be sufficient to be replayed against an ingester.

Here is an example for a zmq SUB socket

```python
import zmq
import sys
import pickle

if len(sys.argv) < 4:
    print("usage: host port file")
    sys.exit(0)

context = zmq.Context()

sock = context.socket(zmq.SUB)
sock.connect ("tcp://%s:%u" % (sys.argv[1], int(sys.argv[2])))
sock.setsockopt(zmq.SUBSCRIBE, b"")

f = open(sys.argv[3], "ab")
try:
    while True:
        parts = sock.recv_multipart()
        print(len(parts), parts[0])
        pickle.dump(parts, f)
except KeyboardInterrupt:
    print("closing file")
    f.close()
```

This script receives zmq Multipart messages from a publisher and writes them to a sequential pickle file.
While dranspose internally uses zmq, ingesters can be used to receive data from other stream formats.

## Internal Stream

All ingesters derived from the [Ingester](../reference/internals/ingester.md) class accept a `dump_path` setting.
If this is set, the ingester will write all data which is sent out to the workers also to a file.


## Pickles Format

As pickled objects have a length information in the header, it is possible to write consecutive pickle dumps into the same file.
The file is then read without any seeking like this:

```python
pkgs = []
with open("ingest-dump.pkls","rb") as f:
    while True:
        try:
            frames = pickle.load(f)
            pkgs.append(frames)
        except EOFError:
            break
```