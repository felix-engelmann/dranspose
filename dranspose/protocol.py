import pickle

import zmq

class ProtocolException(Exception):
    pass

PREFIX = "dranspose"