import pickle

from dranspose.data.albaem import AlbaemPacket
from dranspose.data.contrast import ContrastPacket
from dranspose.data.eiger_legacy import EigerLegacyPacket
from dranspose.data.xspress3 import XspressPacket, XspressImage


def test_contrast_stream() -> None:
    with open("tests/data/contrast-dump.pkls", "rb") as f:
        while True:
            try:
                frames = pickle.load(f)
                assert len(frames) == 1
                data = pickle.loads(frames[0])
                pkg = ContrastPacket.validate_python(data)
                print(pkg)
            except EOFError:
                break


def test_xspress3_stream() -> None:
    with open("tests/data/xspress3-dump.pkls", "rb") as f:
        skip = 0
        while True:
            try:
                frames = pickle.load(f)
                assert len(frames) == 1
                if skip > 0:
                    skip += 1
                    continue
                pkg = XspressPacket.validate_json(frames[0])
                print(pkg)
                if isinstance(pkg, XspressImage):
                    skip = 2
            except EOFError:
                break


def test_albaem_stream() -> None:
    with open("tests/data/albaem-dump.pkls", "rb") as f:
        while True:
            try:
                frames = pickle.load(f)
                assert len(frames) == 1
                pkg = AlbaemPacket.validate_json(frames[0])
                print(pkg)
            except EOFError:
                break


def test_eiger_legacy_stream() -> None:
    with open("tests/data/eiger-small.pkls", "rb") as f:
        while True:
            try:
                frames = pickle.load(f)
                pkg = EigerLegacyPacket.validate_json(frames[0])
                print(pkg)
            except EOFError:
                break
