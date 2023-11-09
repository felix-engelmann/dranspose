import pickle

import zmq

class ProtocolException(Exception):
    pass

async def ping(sock: zmq.Socket, addr: bytes):
    await sock.send_multipart([addr, Protocol.PING])


async def pong(sock: zmq.Socket, state):
    await sock.send_multipart([Protocol.PONG, pickle.dumps(state)])


async def parse(data: list[bytes]):
    if len(data) < 1:
        raise ProtocolException()

    if data[0] == Protocol.PONG:
        if len(data) < 2:
            raise ProtocolException("PONG has no payload")
        return {"type": Protocol.PONG, "data": pickle.loads(data[1])}
    elif data[0] == Protocol.PING:
        return {"type": Protocol.PING}
    else:
        raise ProtocolException("unknown message")

class Protocol:
    PING = b'ping'
    PONG = b'pong'
