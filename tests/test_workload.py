import asyncio
import datetime
import logging
from typing import Awaitable, Callable, Optional

import aiohttp
import numpy as np

import pytest
import zmq.asyncio
import zmq
from psutil._ntuples import snicaddr, snetio

from dranspose.workload_generator import Statistics, NetworkConfig
from tests.utils import consume_zmq


@pytest.mark.parametrize("srv,cli", [(zmq.PUSH, zmq.PULL), (zmq.PUB, zmq.SUB)])
@pytest.mark.asyncio
async def test_debugger(
    workload_generator: Callable[[Optional[int]], Awaitable[None]], srv: int, cli: int
) -> None:
    await workload_generator(5003)

    nframes = 5

    ctx = zmq.asyncio.Context()
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5003/api/v1/config")
        state = NetworkConfig.model_validate_json(await st.read())
        for ifname, ifaddrs in state.addresses.items():
            logging.info("interface %s : %s", ifname, state.stats[ifname])
            for ifaddr in ifaddrs:
                logging.info("    %s", snicaddr(*ifaddr))

        st = await session.get("http://localhost:5003/api/v1/finished")
        state = await st.json()
        logging.info("gen state %s", state)

        task = asyncio.create_task(consume_zmq(ctx, nframes + 2, typ=cli))
        logging.info("created consumer task")

        st = await session.post(
            "http://localhost:5003/api/v1/open_socket",
            json={"type": srv, "port": 9999},
        )
        state = await st.json()
        logging.info("open %s", state)

        await asyncio.sleep(1)

        st = await session.post(
            "http://localhost:5003/api/v1/frames",
            json={"number": nframes, "time": 0.0001, "shape": [100, 100]},
        )
        state = await st.json()
        st.close()
        logging.info("sending frames %s", state)

        st = await session.get("http://localhost:5003/api/v1/finished")
        state = await st.json()
        st.close()
        while not state:
            await asyncio.sleep(0.5)
            st = await session.get("http://localhost:5003/api/v1/finished")
            state = await st.json()
            st.close()

            st = await session.get("http://localhost:5003/api/v1/statistics")
            stat = await st.json()
            st.close()
            logging.info("fps %s", stat["fps"])

        st = await session.post("http://localhost:5003/api/v1/close_socket")
        state = await st.json()
        st.close()
        logging.info("close %s", state)

        st = await session.get("http://localhost:5003/api/v1/statistics")
        stat = Statistics.model_validate_json(await st.read())
        st.close()
        last = {}
        last_sent = 0
        for t, sent, stats in stat.snapshots:
            logging.info(
                "timestamp %s: sent %d, Δ%d",
                datetime.datetime.fromtimestamp(t),
                sent,
                sent - last_sent,
            )
            last_sent = sent
            for ifname, nst in stats.items():
                if ifname not in last:
                    last[ifname] = nst
                    logging.info("    %s: %s", ifname, nst)
                else:
                    delta = snetio(*map(lambda x: x[1] - x[0], zip(last[ifname], nst)))
                    logging.info("   Δ%s: %s", ifname, delta)
                    last[ifname] = nst
    await task
    ctx.destroy()


@pytest.mark.asyncio
async def test_sink(
    workload_generator: Callable[[Optional[int]], Awaitable[None]]
) -> None:
    await workload_generator(5003)
    logging.debug("start gen at 5004")
    await workload_generator(5004)

    async with aiohttp.ClientSession() as session:
        nframes = 100

        logging.debug("opening PUSH on 9999 at 5003")

        st = await session.post(
            "http://localhost:5003/api/v1/open_socket",
            json={"type": zmq.PUSH, "port": 9999},
        )
        state = await st.json()
        logging.info("open %s", state)

        logging.debug("connect PULL to 127.0.0.1:9999 at 5004")
        st = await session.post(
            "http://localhost:5004/api/v1/connect_socket",
            json={"type": zmq.PULL, "url": "tcp://127.0.0.1:9999"},
        )
        state = await st.json()
        logging.info("open %s", state)

        await asyncio.sleep(0.4)

        st = await session.post(
            "http://localhost:5003/api/v1/frames",
            json={"number": nframes, "time": 0.01, "shape": [100, 100]},
        )
        state = await st.json()
        logging.info("sending frames %s", state)

        st = await session.get("http://localhost:5003/api/v1/finished")
        state = await st.json()
        logging.info("is finished %s", state)
        while not state:
            await asyncio.sleep(0.5)
            st = await session.get("http://localhost:5003/api/v1/finished")
            state = await st.json()

            st = await session.get("http://localhost:5003/api/v1/statistics")
            stat = await st.json()
            logging.info("sender fps %s", stat["fps"])

            st = await session.get("http://localhost:5004/api/v1/statistics")
            stat = await st.json()
            logging.info("receiver fps %s", stat["fps"])
            logging.info(
                "receiver latencies mean %f, max %f",
                np.mean(stat["deltas"]),
                max(stat["deltas"]),
            )

        st = await session.post("http://localhost:5003/api/v1/close_socket")
        state = await st.json()
        logging.info("close source %s", state)

        st = await session.post("http://localhost:5004/api/v1/close_socket")
        state = await st.json()
        logging.info("close sink %s", state)
