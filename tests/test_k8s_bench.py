import asyncio
import datetime
import json
import logging
import time
from collections import defaultdict

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


@pytest.mark.parametrize("ingester", ["fast"])  # "fast", "large",small
@pytest.mark.parametrize("size", [1000])
@pytest.mark.skipif(
    "not config.getoption('k8s')",
    reason="explicitly enable --k8s remote bench, optional --plots",
)
@pytest.mark.asyncio
async def est_single_ingester(ingester, plt, size) -> None:
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

        ntrig = 20000
        delay = 0.000001
        resp = await session.post(
            get_url("mapping"),
            json={
                ingester: [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i // 10)).model_dump(
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
            json={"number": ntrig, "time": delay, "shape": [size, size]},
        )
        state = await st.json()
        logging.info("sending frames %s", state)

        worker_times = defaultdict(list)
        ingester_times = defaultdict(list)
        measured_times = []

        exp_start = None

        st = await session.get(get_url("progress"))
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.8)
            st = await session.get(get_url("progress"))
            content = await st.json()
            logging.info("progress %s", content)

            st = await session.get(get_url("config"))
            if exp_start is None:
                exp_start = time.time()
            measured_times.append(time.time() - exp_start)
            conf = EnsembleState.model_validate(await st.json())
            msg = []
            for k in conf.workers:
                worker_times[k.name].append(k.event_rate)
            for k in conf.ingesters:
                ingester_times[k.name].append(k.event_rate)
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

        bytes_times = []
        sent_times = []
        stat_times = []
        ifname = "purple0"
        for i in range(len(stat.snapshots) - 1, 0, -1):
            t, sent, stats = stat.snapshots[i]
            tp, sentp, statsp = stat.snapshots[i - 1]
            stat_times.append(t - exp_start)
            bytes_times.append(
                snetio(
                    *map(lambda x: x[1] - x[0], zip(statsp[ifname], stats[ifname]))
                ).bytes_sent
                / (t - tp)
                / 1024
                / 1024
            )
            sent_times.append((sent - sentp) / (t - tp))
            if t < exp_start and sent - sentp == 0:
                break

        plt.figure(figsize=(10, 10))

        for w in worker_times:
            plt.plot(measured_times, worker_times[w], label=w)
        plt.plot(
            measured_times,
            list(map(sum, zip(*worker_times.values()))),
            label="worker sum",
        )
        for i in ingester_times:
            plt.plot(measured_times, ingester_times[i], label=i)
        plt.plot(stat_times, sent_times, label="generated packets")
        plt.plot(stat_times, bytes_times, label="outgoing MiBi/s")
        plt.title(f"{ntrig} frames, {delay}s delay, {size*size*2} bytes/f")
        plt.ylabel("pakets/s")
        plt.xlabel("time in s")
        plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1))


@pytest.mark.parametrize("size", [1000])
@pytest.mark.skipif(
    "not config.getoption('k8s')",
    reason="explicitly enable --k8s remote bench, optional --plots",
)
@pytest.mark.asyncio
async def est_dual_ingester(plt, size) -> None:
    async with aiohttp.ClientSession() as session:
        ingesters = ["fast", "large"]
        st = await session.get(get_url("config"))
        state = EnsembleState.model_validate(await st.json())
        logging.info("available streams %s", state.get_streams())

        for ing in ingesters:
            await session.post(
                get_gen("open_socket", svc=ing),
                json={"type": zmq.PUSH, "port": 9999},
            )

        await asyncio.sleep(1)

        ntrig = 20000
        delay = 0.00000001
        await session.post(
            get_url("mapping"),
            json={
                ing: [
                    [
                        VirtualWorker(constraint=VirtualConstraint(i // 10)).model_dump(
                            mode="json"
                        )
                    ]
                    for i in range(1, ntrig)
                ]
                for ing in ingesters
            },
        )

        await asyncio.sleep(0.4)

        for ing in ingesters:
            await session.post(
                get_gen("frames", svc=ing),
                json={"number": ntrig, "time": delay, "shape": [size, size]},
            )
        logging.info("sending frames %s", state)

        worker_times = defaultdict(list)
        ingester_times = defaultdict(list)
        measured_times = []

        exp_start = None

        st = await session.get(get_url("progress"))
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.8)
            st = await session.get(get_url("progress"))
            content = await st.json()
            logging.info("progress %s", content)

            st = await session.get(get_url("config"))
            if exp_start is None:
                exp_start = time.time()
            measured_times.append(time.time() - exp_start)
            conf = EnsembleState.model_validate(await st.json())
            msg = []
            for k in conf.workers:
                worker_times[k.name].append(k.event_rate)
            for k in conf.ingesters:
                ingester_times[k.name].append(k.event_rate)
            for k in conf.workers + conf.ingesters + [conf.reducer]:
                msg.append(f"{k.name}:{k.processed_events} -- {k.event_rate}")
            logging.info("state is \n%s", "\n".join(msg))

        st = await session.post(get_gen("close_socket", svc=ingesters[0]))
        st = await session.post(get_gen("close_socket", svc=ingesters[1]))
        state = await st.json()
        logging.info("close %s", state)

        bytes_times = defaultdict(list)
        sent_times = defaultdict(list)
        stat_times = defaultdict(list)

        for ing in ingesters:
            st = await session.get(get_gen("statistics", svc=ing))
            stat = Statistics.model_validate_json(await st.read())

            for i in range(len(stat.snapshots) - 1, 0, -1):
                t, sent, stats = stat.snapshots[i]
                tp, sentp, statsp = stat.snapshots[i - 1]
                stat_times[ing].append(t - exp_start)
                bytes_times[ing].append(
                    snetio(
                        *map(lambda x: x[1] - x[0], zip(statsp["eth0"], stats["eth0"]))
                    ).bytes_sent
                    / (t - tp)
                    / 1024
                    / 1024
                )
                sent_times[ing].append((sent - sentp) / (t - tp))
                if t < exp_start and sent - sentp == 0:
                    break

        plt.figure(figsize=(10, 10))

        for w in worker_times:
            plt.plot(measured_times, worker_times[w], label=w)
        plt.plot(
            measured_times,
            list(map(sum, zip(*worker_times.values()))),
            label="worker sum",
        )
        for i in ingester_times:
            plt.plot(measured_times, ingester_times[i], label=i)
        for ing in ingesters:
            plt.plot(stat_times[ing], sent_times[ing], label="generated packets")
            plt.plot(stat_times[ing], bytes_times[ing], label="outgoing MiBi/s")
        plt.title(f"{ntrig} frames, {delay}s delay, {size*size*2} bytes/f")
        plt.ylabel("pakets/s")
        plt.xlabel("time in s")
        plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1))


@pytest.mark.parametrize(
    "size", [1024, 512, 256, 128, 64, 32, 16, 8]
)  # 1024, 512, 256, 128, 64, 32, 16, 8
@pytest.mark.skipif(
    "not config.getoption('k8s')",
    reason="explicitly enable --k8s remote bench, optional --plots",
)
@pytest.mark.asyncio
async def test_raw_zmq(plt, size) -> None:
    async with aiohttp.ClientSession() as session:
        st = await session.get(get_url("config"))
        state = EnsembleState.model_validate(await st.json())
        logging.info("available streams %s", state.get_streams())

        exp_start = time.time()

        maxfps = 20000
        if size < 256:
            nframes = maxfps * 10
        else:
            nframes = 50000

        await session.post(
            get_gen("open_socket", svc="large"),
            json={"type": zmq.PUSH, "port": 9999},
        )
        await asyncio.sleep(1)

        st = await session.post(
            get_gen("connect_socket", svc="small"),
            json={"type": zmq.PULL, "url": "tcp://172.18.11.224:9999"},
        )
        state = await st.json()
        logging.info("open %s", state)

        st = await session.post(
            get_gen("frames", svc="large"),
            json={"number": nframes, "time": 0.0000001, "shape": [size, size]},
        )
        state = await st.json()
        logging.info("sending frames %s", state)

        st = await session.get(get_gen("finished", svc="large"))
        state = await st.json()
        logging.info("is finished %s", state)
        while not state:
            await asyncio.sleep(0.5)
            st = await session.get(get_gen("finished", svc="large"))
            state = await st.json()

            st = await session.get(get_gen("statistics", svc="large"))
            stat = await st.json()
            logging.info("sender fps %s", stat["fps"])

            st = await session.get(get_gen("statistics", svc="small"))
            stat = await st.json()
            logging.info("receiver fps %s", stat["fps"])

        st = await session.post(get_gen("close_socket", svc="large"))
        state = await st.json()
        logging.info("close source %s", state)

        st = await session.post(get_gen("close_socket", svc="small"))
        state = await st.json()
        logging.info("close sink %s", state)

        bytes_tx_times = defaultdict(list)
        bytes_rx_times = defaultdict(list)
        sent_times = defaultdict(list)
        stat_times = defaultdict(list)

        ingesters = ["large", "small"]
        for ing in ingesters:
            st = await session.get(get_gen("statistics", svc=ing))
            stat = Statistics.model_validate_json(await st.read())

            ifname = "purple0"
            for i in range(len(stat.snapshots) - 1, 0, -1):
                t, sent, stats = stat.snapshots[i]
                tp, sentp, statsp = stat.snapshots[i - 1]
                stat_times[ing].append(t - exp_start)
                bytes_tx_times[ing].append(
                    snetio(
                        *map(lambda x: x[1] - x[0], zip(statsp[ifname], stats[ifname]))
                    ).bytes_sent
                    / (t - tp)
                    / 1024
                    / 1024
                )
                bytes_rx_times[ing].append(
                    snetio(
                        *map(lambda x: x[1] - x[0], zip(statsp[ifname], stats[ifname]))
                    ).bytes_recv
                    / (t - tp)
                    / 1024
                    / 1024
                )
                sent_times[ing].append((sent - sentp) / (t - tp))
                if t < exp_start and sent - sentp == 0:
                    break

        with open(f"bench/raw_zmq_{size*size*2}.json", "w") as f:
            json.dump(
                {
                    "times": stat_times,
                    "packets": sent_times,
                    "tx": bytes_tx_times,
                    "rx": bytes_rx_times,
                },
                f,
            )
        for ing in ingesters:
            plt.plot(stat_times[ing], sent_times[ing], label=f"generated packets {ing}")
            plt.plot(
                stat_times[ing], bytes_tx_times[ing], label=f"outgoing MiBi/s {ing}"
            )
            plt.plot(
                stat_times[ing], bytes_rx_times[ing], label=f"incoming MiBi/s {ing}"
            )
        plt.title(f"{nframes} frames, no delay, {size*size*2/1024} kbytes/f")
        plt.ylabel("pakets/s")
        plt.xlabel("time in s")
        plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1))
