import asyncio
import logging
import time

import pytest
import zmq.asyncio


@pytest.mark.asyncio
async def test_zmq_multiroute(time_beacon) -> None:
    async def dealer_msink(ports, name, n) -> None:
        c = zmq.asyncio.Context()
        s = c.socket(zmq.DEALER)
        s.setsockopt(zmq.IDENTITY, name)
        for port in ports:
            s.connect(f"tcp://localhost:{port}")
        for _ in ports:
            await s.send(b"ping")
        start = time.perf_counter()
        total_bytes = 0
        for i in range(n):
            logging.debug("sink recv %d", i)
            data = await s.recv_multipart(copy=True)
            total_bytes += sum(map(len, data))
            if i % 1000 == 0:
                end = time.perf_counter()
                logging.info(
                    "%s: sink 100 packets took %s: %lf p/s",
                    name,
                    end - start,
                    1000 / (end - start),
                )
                start = end
            # logging.info("i %d, data %s", i, data)
        logging.info("total bytes %d", total_bytes)
        c.destroy()

    async def route(inport, outport, n, workers) -> None:
        c = zmq.asyncio.Context()
        sin = c.socket(zmq.PULL)
        sout = c.socket(zmq.ROUTER)
        sin.connect(f"tcp://localhost:{inport}")
        sout.bind(f"tcp://*:{outport}")
        for _ in workers:
            data = await sout.recv_multipart()
            logging.info("%d: registered worker %s", inport, data)
        start = time.perf_counter()
        for i in range(n):
            data = await sin.recv_multipart(copy=False)
            await sout.send_multipart([workers[i % len(workers)]] + data)
            if i % 1000 == 0:
                end = time.perf_counter()
                logging.info(
                    "%d: forward 1000 packets took %s: %lf p/s",
                    inport,
                    end - start,
                    1000 / (end - start),
                )
                start = end
        c.destroy()

    pkts = 10000
    ctask = asyncio.create_task(dealer_msink([9998, 9918], b"w1", 2 * pkts))
    r1task = asyncio.create_task(route(9999, 9998, pkts, [b"w1"]))
    r2task = asyncio.create_task(route(9919, 9918, pkts, [b"w1"]))

    context = zmq.asyncio.Context()
    s1task = asyncio.create_task(time_beacon(context, 9999, pkts, 0.00001))
    s2task = asyncio.create_task(time_beacon(context, 9919, pkts, 0.00001))

    await s1task
    await s2task

    await ctask
    await r1task
    await r2task

    context.destroy()
