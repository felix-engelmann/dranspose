import subprocess
import time

import pytest


def test_simple() -> None:
    p = subprocess.Popen(["dranspose"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(4)
    p.terminate()
    text = p.communicate()[0]
    assert "usage: dranspose" in text.decode()


@pytest.mark.parametrize(
    "cmd, output",
    [
        (["controller"], "Uvicorn running on"),
        (
            ["reducer", "-c", "examples.dummy.reducer:FluorescenceReduce"],
            "created reducer with state service_uuid=UUID",
        ),
        (
            ["worker", "-c", "examples.dummy.worker:FluorescenceWorker"],
            "created worker with state service_uuid=UUID",
        ),
        (
            [
                "ingester",
                "-c",
                "TcpPcapIngester",
                "-u",
                "tcp://127.0.0.1:8888",
                "-n",
                "bla",
            ],
            "ingester:all subtasks running",
        ),
        (
            [
                "http_ingester",
                "-c",
                "dranspose.ingesters.http_sardana:app",
                "-n",
                "sardana",
            ],
            "Uvicorn running on",
        ),
        (["combined"], "worker2:registered ready message"),
        (["debugworker"], "Uvicorn running on"),
    ],
)
def test_component(cmd: list[str], output: str) -> None:
    p = subprocess.Popen(
        ["dranspose", *cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    time.sleep(4)
    p.terminate()
    text = b"".join(p.communicate())
    assert output in text.decode()
