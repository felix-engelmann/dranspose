import asyncio
import time

import pytest
import pytest_asyncio
import uvicorn

from dranspose.controller import app
from dranspose.ingesters.streaming_single import StreamingSingleIngester
from dranspose.worker import Worker

import redis.asyncio as redis


@pytest.mark.asyncio
async def test_simple():
    await asyncio.sleep(0.5)

@pytest_asyncio.fixture()
async def controller():
    config = uvicorn.Config(app, port=5000, log_level="info")
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

