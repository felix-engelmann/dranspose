from typing import Literal

from pydantic import BaseModel, TypeAdapter, ConfigDict


class SardanaDataDescription(BaseModel):
    """
    Example:
        ```python
        SardanaDataDescription(
            type='DataDescription',
            serialno=20338,
            scandir='/data/staff/femtomax/20230413',
            title='burstscan dummy_mot_01 0.0 0.1 2 50',
            column_desc=[{
                'name': 'point_nb',
                'label': '#Pt No',
                'dtype':
                'int64',
                'shape': []
                }, {
                'min_value': 0,
                'max_value': 0.1,
                'instrument': '',
                'name': 'dummy_mot_01',
                'label': 'dummy_mot_01',
                'dtype': 'float64',
                'shape': [],
                'is_reference': True
                }, {
                'name': 'timestamp',
                'label': 'dt',
                'dtype': 'float64',
                'shape': []}
            ],
            ref_moveables=['dummy_mot_01'],
            estimatedtime=-2.6585786096205566,
            total_scan_intervals=2,
            starttime='Mon Apr 17 14:19:35 2023',
            counters=['tango://b-v-femtomax-csdb-0.maxiv.lu.se:10000/expchan/oscc_02_seq_ctrl/1',
            'tango://b-v-femtomax-csdb-0.maxiv.lu.se:10000/expchan/panda_femtopcap_ctrl/4',
            'tango://b-v-femtomax-csdb-0.maxiv.lu.se:10000/expchan/panda_femtopcap_ctrl/5'],
            scanfile=['stream.daq', 'tests_03.h5']
        )
        ```
    """

    model_config = ConfigDict(extra="allow")
    type: Literal["DataDescription"] = "DataDescription"
    serialno: int
    scandir: str
    title: str


class SardanaRecordData(BaseModel):
    """
    Example:
        ```python
        SardanaRecordData(
            type='RecordData',
            timestamp=0.6104741096496582,
            point_nb=8,
            dummy_mot_01=0
        )
        ```
    """

    model_config = ConfigDict(extra="allow")
    type: Literal["RecordData"] = "RecordData"
    timestamp: float


class SardanaRecordEnd(BaseModel):
    """
    Example:
        ```python
        SardanaRecordEnd(type='RecordEnd')
        ```
    """

    model_config = ConfigDict(extra="allow")
    type: Literal["RecordEnd"] = "RecordEnd"


SardanaPacketType = SardanaDataDescription | SardanaRecordData | SardanaRecordEnd

SardanaPacket = TypeAdapter(
    SardanaDataDescription | SardanaRecordData | SardanaRecordEnd
)
"""
Union type for Sardana packets
"""
