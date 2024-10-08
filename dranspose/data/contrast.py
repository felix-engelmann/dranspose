import pickle
from typing import Literal

import zmq
from pydantic import BaseModel, TypeAdapter, ConfigDict

from dranspose.event import StreamData


class ContrastBase(BaseModel):
    def to_stream_data(self) -> StreamData:
        dat = pickle.dumps(self.model_dump())
        return StreamData(typ="contrast", frames=[zmq.Frame(dat)])


class ContrastStarted(ContrastBase):
    """
    Example:
        ``` py
        ContrastStarted(
            status='started',
            path='/data/.../diff_1130_stream_test/raw/dummy',
            scannr=2,
            description='mesh sx -2 2 3 sy -2 2 4 0.1',
            snapshot={'attenuator1_x': 0.048,
                      'attenuator2_x': 0.067,
                      'attenuator3_x': 0.536,
                      'vfm_yaw': 0.15148492851039919,
                      'xrf_x': 94.99875}
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    status: Literal["started"] = "started"
    path: str
    scannr: int
    description: str


class ContrastRunning(ContrastBase):
    """
    Example:
        ``` py
        ContrastRunning(
            status='running',
            dt=2.410903215408325,
            sx=-2.000431059888797,
            sy=-2.0011940002441406,
            pseudo={
                'x': array([-2.00405186]),
                'y': array([-2.00290304]),
                'z': array([0.00029938]),
                'analog_x': array([-1.99962707]),
                'analog_y': array([-1.99349905]),
                'analog_z': array([-0.00306218])
            },
            panda0={
                'COUNTER1.OUT_Value': array([0.]),
                'COUNTER2.OUT_Value': array([0.]),
                'COUNTER3.OUT_Value': array([0.]),
                'FMC_IN.VAL6_Mean': array([0.04644599]),
                'FMC_IN.VAL7_Mean': array([-0.02943451]),
                'FMC_IN.VAL8_Mean': array([0.01255371])
            },
            xspress3={
                'type': 'Link',
                'filename': '/data/.../diff_1130_stream_test/raw/dummy/scan_000002_xspress3.hdf5',
                'path': '/entry/instrument/xspress3/',
                'universal': True
            }
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    status: Literal["running"] = "running"
    dt: float


class ContrastFinished(ContrastBase):
    """
    Example:
        ```python
        ContrastFinished(
            status='finished',
            path='/data/.../diff_1130_stream_test/raw/dummy',
            scannr=2,
            description='mesh sx -2 2 3 sy -2 2 4 0.1',
            snapshot={'attenuator1_x': 0.048,
                      'attenuator2_x': 0.066,
                      'vfm_yaw': 0.15148492851039919,
                      'xrf_x': 94.99875}
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    status: Literal["finished"] = "finished"
    path: str
    scannr: int
    description: str


class ContrastHeartbeat(ContrastBase):
    """
    Heartbeat message
    """

    status: Literal["heartbeat"]


ContrastPacket = TypeAdapter(
    ContrastStarted | ContrastRunning | ContrastFinished | ContrastHeartbeat
)
"""
A union type for contrast packets
"""
