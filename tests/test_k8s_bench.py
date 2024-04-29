import asyncio
import datetime
import logging
import aiohttp
import pytest
import zmq
from psutil._common import snetio

from dranspose.protocol import EnsembleState, VirtualWorker, VirtualConstraint
from dranspose.workload_generator import NetworkConfig, Statistics


def get_url(path: str, svc: str = "controller") -> str:
    return f"http://dranspose-bench-{svc}.daq.maxiv.lu.se/api/v1/{path}"


def get_gen(path, svc: str = "large"):
    return f"http://dranspose-workload-generator-{svc}.daq.maxiv.lu.se/api/v1/{path}"


@pytest.mark.parametrize("ingester", ["fast", "large", "small"])
@pytest.mark.skipif(
    "not config.getoption('k8s')", reason="explicitly enable --k8s remote bench"
)
@pytest.mark.asyncio
async def test_single_ingester(ingester) -> None:
    async with aiohttp.ClientSession() as session:
        st = await session.get(get_url("config"))
        state = EnsembleState.model_validate(await st.json())
        logging.info("available streams %s", state.get_streams())

        st = await session.get(get_gen("config", svc=ingester))
        state = NetworkConfig.model_validate(await st.json())
        logging.info("config is %s", state)

        st = await session.get(get_gen("statistics", svc=ingester))
        state = Statistics.model_validate(await st.json())
        logging.info("stat is %s, %s", len(state.snapshots), state.snapshots[-1])

        st = await session.post(
            get_gen("open_socket", svc=ingester),
            json={"type": zmq.PUSH, "port": 9999},
        )
        state = await st.json()
        logging.info("open %s", state)

        await asyncio.sleep(1)

        ntrig = 100
        resp = await session.post(
            get_url("mapping"),
            json={
                ingester: [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(1, ntrig)
                ],
            },
        )
        assert resp.status == 200
        uuid = await resp.json()

        logging.info("mapping uuid is %s", uuid)

        await asyncio.sleep(0.4)

        st = await session.post(
            get_gen("frames", svc=ingester),
            json={"number": ntrig, "time": 0.1, "shape": [100, 100]},
        )
        state = await st.json()
        logging.info("sending frames %s", state)

        st = await session.get(get_url("progress"))
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get(get_url("progress"))
            content = await st.json()
            logging.info("progress %s", content)

        st = await session.get(get_url("config"))
        conf = EnsembleState.model_validate(await st.json())
        msg = []
        for k in conf.workers + conf.ingesters + [conf.reducer]:
            msg.append(f"{k.name}:{k.processed_events} -- {k.event_rate}")
        logging.info("state is \n%s", "\n".join(msg))

        st = await session.post(get_gen("close_socket", svc=ingester))
        state = await st.json()
        logging.info("close %s", state)

        st = await session.get(get_gen("statistics", svc=ingester))
        stat = Statistics.model_validate_json(await st.read())
        last = {}
        last_sent = 0
        for t, sent, stats in stat.snapshots:
            if sent - last_sent == 0:
                continue
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
