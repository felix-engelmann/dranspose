import asyncio
import logging
import random
import time
import aiohttp

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
async def test_map(controller, create_worker, create_ingester):

    await create_worker("w1")
    await create_worker("w2")
    await create_worker("w3")
    await create_ingester(StreamingSingleIngester(connect_url="tcp://localhost:9999", name="eiger"))

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, protocol=3)

    async with aiohttp.ClientSession() as session:
        st = await session.get('http://localhost:5000/api/v1/streams')
        content = await st.json()
        while "eiger" not in content:
            await asyncio.sleep(0.3)
            st = await session.get('http://localhost:5000/api/v1/streams')
            content = await st.json()


        print("startup done")
        resp = await session.post("http://localhost:5000/api/v1/mapping",
                             json={"eiger": [[3], [5], [7], [9], [11], [13], [15], [17], [19]],
                                   #"slow": [None, None, [1006], None, None, [1012], None, None, [1018]]
                                   })
        uuid = await resp.json()

    updates = await r.xread({'dranspose:controller:updates':0})
    print("updates", updates)
    keys = await r.keys("dranspose:*")
    present_keys = {f'dranspose:assigned:{uuid}',f'dranspose:ready:{uuid}'}
    assert present_keys - set(keys) == set()

    context = zmq.asyncio.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind('tcp://*:9999')
    await socket.send_json({'htype': 'header'})
    width = 1475
    height = 831
    for _ in range(9):
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

    readies = await r.xread({f'dranspose:ready:{uuid}': 0})
    while f'dranspose:ready:{uuid}' not in readies or len(readies[f'dranspose:ready:{uuid}'][0]) < 9+3:
        await asyncio.sleep(0.5)
        readies = await r.xread({f'dranspose:ready:{uuid}': 0})

    context.destroy()

    readies = await r.xread({f'dranspose:ready:{uuid}': 0, f'dranspose:assigned:{uuid}':0})
    print("readies", readies)

    await r.aclose()