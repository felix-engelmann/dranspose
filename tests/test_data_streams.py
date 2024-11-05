import pickle

from dranspose.data.albaem import AlbaemPacket
from dranspose.data.contrast import (
    ContrastPacket,
    ContrastHeartbeat,
    ContrastStarted,
    ContrastRunning,
    ContrastFinished,
)
from dranspose.data.eiger_legacy import (
    EigerLegacyPacket,
    EigerLegacyHeader,
    EigerLegacyImage,
)
from dranspose.data.xspress3 import XspressPacket, XspressImage, XspressStart


def test_contrast_stream() -> None:
    with open("tests/data/contrast-dump.pkls", "rb") as f:
        while True:
            try:
                frames = pickle.load(f)
                assert len(frames) == 1
                data = pickle.loads(frames[0])
                pkg = ContrastPacket.validate_python(data)
                if isinstance(pkg, ContrastHeartbeat):
                    pass
                elif isinstance(pkg, ContrastStarted):
                    assert pkg == ContrastStarted(
                        path="/data/staff/nanomax/commissioning_2023-2/diff_1130_stream_test/raw/dummy",
                        scannr=2,
                        description="mesh sx -2 2 3 sy -2 2 4 0.1",
                        snapshot={
                            "attenuator3_x": 0.536,
                            "table_back_x": -50.54900000000001,
                            "table_front_y": 10.907449999999997,
                            "ivu_gap": 37.29985500000001,
                            "basez": 544.3149999999987,
                            "basey": 9175.296999999999,
                            "sy": 1.9996681213378906,
                            "ivu_taper": -0.000345000000006479,
                            "table_back_y": 7.18965,
                            "sz": -0.0021934551186859608,
                            "seh_left": -925.065,
                            "energy_raw": 11971.8386720316,
                            "sx": 1.9986190804920625,
                            "energy": 12000.002585815417,
                            "dummy1": 0.0,
                            "seh_right": 1986.3600000000001,
                            "dummy2": 0.0,
                            "attenuator1_x": 0.048,
                            "attenuator2_x": 0.067,
                            "attenuator4_x": 0.043000000000000003,
                            "diode1_x": 18201.062,
                            "bl_filter_1": 0.0019500000000221007,
                            "bl_filter_2": 35.99755000000005,
                            "pol_x": 0.737,
                            "vfm_x": 0.72199999999998,
                            "pol_y": 1.048,
                            "pol_rot": -0.007130000000017844,
                            "vfm_y": -0.04063853000002382,
                            "vfm_pit": 2.707106597004895,
                            "vfm_yaw": 0.15148492851039919,
                            "hfm_x": -1.5829799999999068,
                            "hfm_y": -1.4000297500000443,
                            "fastshutter_y": -0.1,
                            "hfm_pit": 2.71675126876562,
                            "hfm_bend": 90.16208269715501,
                            "mono_x": -0.10119400000007772,
                            "dbpm1_x": -0.001,
                            "mono_bragg": 9.505422492401216,
                            "mono_x2per": -0.0309500000000007,
                            "dbpm1_y": -0.001,
                            "mono_x2pit": -0.22180038229322463,
                            "seh_posy": -0.015500000000201908,
                            "mono_x2rol": 0.004552690048114982,
                            "seh_gapy": 3499.1639999999998,
                            "seh_posx": 0.006499999999959982,
                            "seh_gapx": 3499.9980000000005,
                            "mono_x2fpit": 6.7092,
                            "skb_posy": 2.98599999999999,
                            "skb_gapy": 382.793,
                            "skb_posx": -1.4455000000000027,
                            "mono_x2frol": 6.155,
                            "skb_gapx": 219.87,
                            "m1froll": 18.99738312,
                            "nanobpm_y": 2.48525,
                            "skb_top": 202.92499999999998,
                            "m1fpitch": 9.992607117,
                            "m2fpitch": 14.12856865,
                            "skb_bottom": 181.97799999999998,
                            "ssa_gapx": 10.036000000000058,
                            "ssa_gapy": 2.0200000000004366,
                            "skb_left": 120.924,
                            "ssa_posx": 348.0,
                            "ssa_posy": -376.3042500001611,
                            "oam_x": 0.0030000000000001137,
                            "skb_right": 112.726,
                            "oam_y": 0.00035000000000096065,
                            "oam_z": 4.999999999988347e-05,
                            "oam_zoom": 11.831569664902998,
                            "pinhole_x": 9.066,
                            "topm_x": -1.2485,
                            "topm_y": -0.22089999999999677,
                            "topm_z": -0.19170000000000087,
                            "pinhole_y": -6.448,
                            "topm_zoom": 0.0,
                            "gontheta": -0.004472222222221212,
                            "gonphi": -0.0031999999999994255,
                            "pinhole_z": 0.186,
                            "gonx1": 145.64500000000044,
                            "gonx2": -1298.4260000000031,
                            "gonx3": -2221.1579999999994,
                            "dbpm2_x": 4799.999,
                            "gony1": 1472.492000000002,
                            "dbpm2_y": 2800.0,
                            "gony2": -1310.1529999999984,
                            "gonz": 24419642.0,
                            "beamstop_x": 1.3710910720877507,
                            "seh_top": 3142.399,
                            "beamstop_y": 0.0,
                            "xrf_x": 94.99875,
                            "table_front_x": -19.636250000000018,
                            "seh_bottom": 340.772,
                            "basex": -281.7609999999986,
                        },
                    )
                elif isinstance(pkg, ContrastRunning):
                    assert pkg.xspress3 == {
                        "type": "Link",
                        "filename": "/data/staff/nanomax/commissioning_2023-2/diff_1130_stream_test/raw/dummy/scan_000002_xspress3.hdf5",
                        "path": "/entry/instrument/xspress3/",
                        "universal": True,
                    }
                else:
                    assert pkg == ContrastFinished(
                        status="finished",
                        path="/data/staff/nanomax/commissioning_2023-2/diff_1130_stream_test/raw/dummy",
                        scannr=2,
                        description="mesh sx -2 2 3 sy -2 2 4 0.1",
                        snapshot={
                            "attenuator3_x": 0.537,
                            "table_back_x": -50.54900000000001,
                            "table_front_y": 10.907449999999997,
                            "ivu_gap": 37.299859999999995,
                            "basez": 544.3159999999989,
                            "basey": 9175.309999999998,
                            "sy": 1.9970932006835938,
                            "ivu_taper": -0.0003449999999993736,
                            "table_back_y": 7.18965,
                            "sz": -0.0005722056957893074,
                            "seh_left": -925.062,
                            "energy_raw": 11971.836775529619,
                            "sx": 1.9993820199451875,
                            "energy": 12000.002585815417,
                            "dummy1": 0.0,
                            "seh_right": 1986.361,
                            "dummy2": 0.0,
                            "attenuator1_x": 0.048,
                            "attenuator2_x": 0.066,
                            "attenuator4_x": 0.043000000000000003,
                            "diode1_x": 18201.063,
                            "bl_filter_1": 0.0020000000000095497,
                            "bl_filter_2": 35.99755000000005,
                            "pol_x": 0.735,
                            "vfm_x": 0.72199999999998,
                            "pol_y": 1.048,
                            "pol_rot": -0.0071389999999951215,
                            "vfm_y": -0.04063853000002382,
                            "vfm_pit": 2.707106597004895,
                            "vfm_yaw": 0.15148492851039919,
                            "hfm_x": -1.5829799999999068,
                            "hfm_y": -1.4000297500000443,
                            "fastshutter_y": -0.1,
                            "hfm_pit": 2.71675126876562,
                            "hfm_bend": 90.16206500090651,
                            "mono_x": -0.10119400000007772,
                            "dbpm1_x": 0.0,
                            "mono_bragg": 9.505420972644377,
                            "mono_x2per": -0.0309500000000007,
                            "dbpm1_y": -0.001,
                            "mono_x2pit": -0.22180038229322463,
                            "seh_posy": -0.015500000000201908,
                            "mono_x2rol": 0.004584527042652553,
                            "seh_gapy": 3499.166,
                            "seh_posx": 0.007000000000061846,
                            "seh_gapx": 3500.0000000000005,
                            "mono_x2fpit": 6.7091,
                            "skb_posy": 2.9839999999999947,
                            "skb_gapy": 382.79300000000006,
                            "skb_posx": -1.445999999999998,
                            "mono_x2frol": 6.1547,
                            "skb_gapx": 219.867,
                            "m1froll": 18.99614334,
                            "nanobpm_y": 2.48525,
                            "skb_top": 202.923,
                            "m1fpitch": 9.996940613,
                            "m2fpitch": 14.12875748,
                            "skb_bottom": 181.97799999999998,
                            "ssa_gapx": 10.03150000000096,
                            "ssa_gapy": 2.0305000000007567,
                            "skb_left": 120.92200000000001,
                            "ssa_posx": 348.0,
                            "ssa_posy": -376.3042500001611,
                            "oam_x": 0.0030000000000001137,
                            "skb_right": 112.723,
                            "oam_y": 0.00035000000000096065,
                            "oam_z": 4.999999999988347e-05,
                            "oam_zoom": 11.831569664902998,
                            "pinhole_x": 9.073,
                            "topm_x": -1.2485,
                            "topm_y": -0.22089999999999677,
                            "topm_z": -0.19170000000000087,
                            "pinhole_y": -6.448,
                            "topm_zoom": 0.0,
                            "gontheta": -0.004472222222221212,
                            "gonphi": -0.0031999999999994255,
                            "pinhole_z": 0.192,
                            "gonx1": 145.65999999999985,
                            "gonx2": -1298.4150000000009,
                            "gonx3": -2221.131999999998,
                            "dbpm2_x": 4800.0,
                            "gony1": 1472.5,
                            "dbpm2_y": 2800.001,
                            "gony2": -1310.137999999999,
                            "gonz": 24419624.0,
                            "beamstop_x": 1.3710910720877507,
                            "seh_top": 3142.388,
                            "beamstop_y": 0.0,
                            "xrf_x": 94.99875,
                            "table_front_x": -19.636250000000018,
                            "seh_bottom": 340.774,
                            "basex": -281.762999999999,
                        },
                    )

            except EOFError:
                break


def test_xspress3_stream() -> None:
    with open("tests/data/xspress3-dump.pkls", "rb") as f:
        skip = 0
        frame = 0
        while True:
            try:
                frames = pickle.load(f)
                assert len(frames) == 1
                if skip > 0:
                    skip += 1
                    continue
                pkg = XspressPacket.validate_json(frames[0])
                if isinstance(pkg, XspressStart):
                    assert (
                        pkg.filename
                        == "/data/staff/nanomax/commissioning_2023-2/diff_1130_stream_test/raw/dummy/scan_000002_xspress3.hdf5"
                    )
                    assert pkg.overwritable is False
                elif isinstance(pkg, XspressImage):
                    assert pkg.frame == frame
                    assert pkg.shape == [4, 4096]
                    assert pkg.exptime == 0.099999875
                    assert pkg.type == "uint32"
                    assert pkg.compression == "none"
                    frame += 1
                    skip = 2
                else:
                    assert (
                        False  # interestingly this capture does not have an end message
                    )
            except EOFError:
                break


def test_albaem_stream() -> None:
    with open("tests/data/albaem-dump.pkls", "rb") as f:
        message_id = 1
        while True:
            try:
                frames = pickle.load(f)
                assert len(frames) == 1
                pkg = AlbaemPacket.validate_json(frames[0])
                assert pkg.message_id == message_id
                message_id += 1
                assert pkg.version == 1
            except EOFError:
                break


def test_eiger_legacy_stream() -> None:
    with open("tests/data/eiger-small.pkls", "rb") as f:
        while True:
            try:
                frames = pickle.load(f)
                pkg = EigerLegacyPacket.validate_json(frames[0])
                if isinstance(pkg, EigerLegacyHeader):
                    assert pkg.header_detail == "all"
                elif isinstance(pkg, EigerLegacyImage):
                    assert pkg.frame >= 0
                else:
                    assert pkg.series == 6
            except EOFError:
                break
