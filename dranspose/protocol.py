import zmq


async def ping(sock: zmq.Socket, addr: bytes):
    await sock.send_multipart([addr, Protocol.PING])


async def pong(sock: zmq.Socket):
    await sock.send_multipart([Protocol.PONG])


class Protocol:
    PING = b'ping'
    PONG = b'pong'
