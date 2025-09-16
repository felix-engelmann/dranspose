from enum import Enum


class DataTypes(Enum):
    CONTRAST = "contrast"
    EIGER_LEGACY = "EIGER_LEGACY"
    LECROY = "lecroy"
    PCAP = "PCAP"
    PCAP_RAW = "PCAP_RAW"
    SARDANA = "sardana"
    STINS = "STINS"
    XSPRESS = "xspress"
