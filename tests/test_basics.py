import asyncio
import logging
import random
import time
import aiohttp
import numpy

import pytest
import pytest_asyncio
import numpy as np
import uvicorn
import zmq.asyncio
import zmq

from dranspose.controller import app
from dranspose.ingesters.streaming_single import StreamingSingleIngester
from dranspose.worker import Worker

import redis.asyncio as redis


@pytest.mark.asyncio
async def test_simple():
    await asyncio.sleep(0.5)

@pytest_asyncio.fixture()
async def controller():
    config = uvicorn.Config(app, port=5000, log_level="debug")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while server.started is False:
        await asyncio.sleep(0.1)
    yield
    server.should_exit = True
    await server_task
    time.sleep(0.1)

@pytest_asyncio.fixture
async def create_worker():
    workers = []

    async def _make_worker(name):
        worker = Worker(name)
        worker_task = asyncio.create_task(worker.run())
        workers.append((worker, worker_task))
        return worker

    yield _make_worker

    for worker, task in workers:
        await worker.close()
        task.cancel()

@pytest_asyncio.fixture
async def create_ingester():
    ingesters = []

    async def _make_ingester(inst):
        ingester_task = asyncio.create_task(inst.run())
        ingesters.append((inst, ingester_task))
        return inst

    yield _make_ingester

    for inst, task in ingesters:
        await inst.close()
        task.cancel()


@pytest_asyncio.fixture
async def stream_eiger():
    async def _make_eiger(ctx, port, nframes):
        socket = ctx.socket(zmq.PUSH)
        socket.bind(f'tcp://*:{port}')
        await socket.send_json({'htype': 'header'})
        width = 1475
        height = 831
        for _ in range(nframes):
            img = np.zeros((width, height), dtype=np.uint16)
            for _ in range(20):
                img[random.randint(0, width - 1)][random.randint(0, height - 1)] = random.randint(0, 10)
            frame = zmq.Frame(img, copy=False)

            await socket.send_json({'htype': 'image',
                                    'shape': img.shape,
                                    'type': 'uint16',
                                    'compression': 'none',
                                    }, flags=zmq.SNDMORE)
            await socket.send(frame, copy=False)
            time.sleep(0.1)
        await socket.send_json({'htype': 'series_end'})
        socket.close()

    yield _make_eiger

@pytest_asyncio.fixture
async def stream_orca():
    async def _make_orca(ctx, port, nframes):
        socket = ctx.socket(zmq.PUSH)
        socket.bind(f'tcp://*:{port}')
        await socket.send_json({'htype': 'header'})
        width = 2000
        height = 4000
        for _ in range(nframes):
            img = np.zeros((width, height), dtype=np.uint16)
            for _ in range(20):
                img[random.randint(0, width - 1)][random.randint(0, height - 1)] = random.randint(0, 10)
            frame = zmq.Frame(img, copy=False)

            await socket.send_json({'htype': 'image',
                                    'shape': img.shape,
                                    'type': 'uint16',
                                    'compression': 'none',
                                    }, flags=zmq.SNDMORE)
            await socket.send(frame, copy=False)
            time.sleep(0.1)
        await socket.send_json({'htype': 'series_end'})
        socket.close()

    yield _make_orca

@pytest_asyncio.fixture
async def stream_alba():
    async def _make_alba(ctx, port, nframes):
        socket = ctx.socket(zmq.PUSH)
        socket.bind(f'tcp://*:{port}')
        await socket.send_json({'htype': 'header'})
        val = np.zeros((0,), dtype=numpy.float64)
        frame = zmq.Frame(val, copy=False)
        for _ in range(nframes):
            await socket.send_json({'htype': 'image',
                                    'shape': val.shape,
                                    'type': 'float64',
                                    'compression': 'none',
                                    }, flags=zmq.SNDMORE)
            await socket.send(frame, copy=False)
            time.sleep(0.1)
        await socket.send_json({'htype': 'series_end'})
        socket.close()

    yield _make_alba

@pytest.mark.asyncio
async def test_services(controller, create_worker, create_ingester):
    print(controller)

    await create_worker("w1")
    await create_worker("w2")
    await create_worker("w3")
    await create_ingester(StreamingSingleIngester(connect_url="tcp://localhost:9999", name="eiger"))

    await asyncio.sleep(2)
    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)
    keys = await r.keys("dranspose:*")
    present_keys = {'dranspose:worker:w2:present', 'dranspose:worker:w3:config', 'dranspose:worker:w1:config', 'dranspose:worker:w3:present', 'dranspose:ingester:eiger_ingester:config', 'dranspose:ingester:eiger_ingester:present', 'dranspose:worker:w2:config', 'dranspose:worker:w1:present'}
    assert present_keys-set(keys) == set()
    await r.aclose()

@pytest.mark.asyncio
async def test_map(controller, create_worker, create_ingester, stream_eiger, stream_orca, stream_alba):

    await create_worker("w1")
    await create_worker("w2")
    await create_worker("w3")
    await create_ingester(StreamingSingleIngester(connect_url="tcp://localhost:9999", name="eiger"))
    await create_ingester(StreamingSingleIngester(connect_url="tcp://localhost:9998", name="orca", worker_port=10011))
    await create_ingester(StreamingSingleIngester(connect_url="tcp://localhost:9997", name="alba", worker_port=10012))
    await create_ingester(StreamingSingleIngester(connect_url="tcp://localhost:9996", name="slow", worker_port=10013))

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

    async with aiohttp.ClientSession() as session:
        st = await session.get('http://localhost:5000/api/v1/streams')
        content = await st.json()
        while {"eiger", "orca", "alba","slow"} - set(content) != set():
            await asyncio.sleep(0.3)
            st = await session.get('http://localhost:5000/api/v1/streams')
            content = await st.json()


        print("startup done")
        ntrig = 10
        resp = await session.post("http://localhost:5000/api/v1/mapping",
                             json={"eiger": [[2*i] for i in range(1, ntrig)],
                                   "orca" : [[2*i+1] for i in range(1, ntrig)],
                                   "alba" : [[2*i, 2*i+1] for i in range(1, ntrig)],
                                   "slow": [[2*i, 2*i+1] if i%4 == 0 else None for i in range(1, ntrig)],
                                   })
        assert resp.status == 200
        uuid = await resp.json()

    updates = await r.xread({'dranspose:controller:updates':0})
    print("updates", updates)
    keys = await r.keys("dranspose:*")
    present_keys = {f'dranspose:assigned:{uuid}',f'dranspose:ready:{uuid}'}
    assert present_keys - set(keys) == set()

    context = zmq.asyncio.Context()

    asyncio.create_task(stream_eiger(context,9999, ntrig-1))
    asyncio.create_task(stream_orca(context,9998, ntrig-1))
    asyncio.create_task(stream_alba(context,9997, ntrig-1))
    asyncio.create_task(stream_alba(context,9996, ntrig//4))

    async with aiohttp.ClientSession() as session:
        st = await session.get('http://localhost:5000/api/v1/status')
        content = await st.json()
        while not content["finished"]:
            await asyncio.sleep(0.3)
            st = await session.get('http://localhost:5000/api/v1/status')
            content = await st.json()

    context.destroy()

    await r.aclose()

    print(content)
