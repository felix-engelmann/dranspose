import pickle

from dranspose.event import StreamData


def parse(data: StreamData):
    assert data.typ == "contrast"
    assert data.length == 1
    return pickle.loads(data.frames[0])