from typing import Any, Optional
import os
import logging

import h5py
import numpy as np

from dranspose.event import EventData
from dranspose.parameters import (
    ParameterType,
    StrParameter,
    # BinaryParameter,
    # ParameterBase
)
from dranspose.protocol import (
    StreamName,
    ParameterName,
    # WorkParameter,
    WorkerState,
    Parameters,
)
from dranspose.data.eiger_legacy import (
    # EigerLegacyEnd,
    EigerLegacyImage,
    EigerLegacyHeader,
)
from dranspose.middlewares.eiger_legacy import parse

logger = logging.getLogger(__name__)


def _create_dataset_nofill(group, name, shape, maxshape, dtype, chunks=None):
    pgroup = "/".join(name.split("/")[:-1])
    try:
        group[pgroup]
    except KeyError:
        group.create_group(pgroup)
    _maxshape = tuple([h5py.h5s.UNLIMITED if sub is None else sub for sub in maxshape])
    spaceid = h5py.h5s.create_simple(shape, _maxshape)
    plist = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    plist.set_fill_time(h5py.h5d.FILL_TIME_NEVER)
    if chunks not in [None, [], ()] and isinstance(chunks, tuple):
        plist.set_chunk(chunks)
    typeid = h5py.h5t.py_create(dtype)
    datasetid = h5py.h5d.create(
        group.file.id, (group.name + "/" + name).encode("utf-8"), typeid, spaceid, plist
    )
    dset = h5py.Dataset(datasetid)
    return dset


class WriterWorker:
    def __init__(
        self, parameters: Parameters, state: WorkerState, **kwargs: Any
    ) -> None:
        self._fh: Optional[h5py.File] = None
        self.name = state.name
        # FIXME parameters not available when worker initialised in test ???
        # do not change stream name once established
        self.stream_name = StreamName(
            "eiger"
        )  # StreamName(parameters["stream_name"].value)
        self._dset_name = f"/entry/instrument/{self.stream_name}/data"
        self._number_dset_name = None

    @staticmethod
    def describe_parameters() -> list[ParameterType]:
        params = [
            # StrParameter(name=ParameterName("stream_name"), default="eiger"),
            StrParameter(name=ParameterName("filename"), default=""),
        ]
        return params

    def save_dict_to_h5(self, data: dict[str, Any], group: h5py.Group) -> None:
        for key, value in data.items():
            if isinstance(value, dict):
                ng = group.create_group(key)
                self.save_dict_to_h5(value, ng)
            else:
                group.create_dataset(key, data=value)

    def open_file(self, meta_header: dict, meta_info: dict, parameters: Parameters):
        ret = {}
        # the parameter has precedence over the stream
        filename = parameters["filename"].value
        if filename == "":
            filename = meta_header.get("filename", "")
        saveraw = meta_info.get("save_raw", True)
        logger.info("Original parameters %s %s", filename, saveraw)
        if filename and saveraw:
            base, ext = os.path.splitext(filename)
            filename = f"{base}_{self.name}{ext}"
            logging.info("%s writing to: %s", self.name, filename)
            if os.path.isfile(filename):
                logger.error("cannot append to existing file")
                self._fh = None
            else:
                try:
                    self._fh = h5py.File(filename, "w")
                except Exception:
                    logger.error("cannot open file %s", filename)
                    self._fh = None
        else:
            self._fh = None
            logger.warning(
                "no file opened. filename: %s save_raw: %s", filename, saveraw
            )
        if self._fh is not None:
            ret["filename"] = filename
            ret["save_raw"] = True
            end = self._dset_name.rfind("/")
            group_name = self._dset_name[:end]
            group = self._fh.create_group(group_name)
            group.attrs["NX_class"] = "NXdetector"
            self.save_dict_to_h5(meta_info, group)
            self._number_dset_name = f"{group_name}/sequence_number"
            group.create_dataset(
                "sequence_number", (0,), maxshape=(None,), dtype=np.uint32
            )
            logger.info(
                "Header: created new file %s with dataset %s",
                filename,
                group_name,
            )
        else:
            ret["filename"] = ""
            ret["save_raw"] = False
        return ret

    def write_frame(self, acq, evt_n) -> None:
        # compression = "bslz4" if "bs" in acq.data["encoding"] else "none"
        # header = {
        #     "htype": "image",
        #     "msg_number": next(self._msg_number),
        #     "frame": header["frame"],
        #     "shape": acq.data["shape"][::-1],
        #     "type": acq.data["type"],
        #     "compression": compression,
        # }
        _shape = acq.data["shape"][::-1]
        _type = acq.data["type"]
        if self._fh is None:
            return
        fh = self._fh
        # FIXME just save the dsets as members
        dset = fh.get(self._dset_name)
        if not dset:
            logger.debug("dataset %s does not exist, creating it", self._dset_name)
            chunks = (1, *acq.data["shape"][::-1])

            logger.debug("chunks are %s", chunks)
            compression, compression_opts = None, None
            if "bs" in acq.data["encoding"]:
                compression = 32008  # bitshuffle.BSHUF_H5FILTER
                compression_opts = (0, 2)  # (0, bitshuffle.BSHUF_H5_COMPRESS_LZ4)

            if compression is None:
                dset = _create_dataset_nofill(
                    fh["/"],
                    name=self._dset_name,
                    shape=(0, *_shape),
                    maxshape=(h5py.h5s.UNLIMITED, *_shape),
                    dtype=_type,
                    chunks=chunks,
                )
            else:
                dset = fh.create_dataset(
                    self._dset_name,
                    dtype=_type,
                    shape=(0, *_shape),
                    maxshape=(None, *_shape),
                    chunks=chunks,
                    compression=compression,
                    compression_opts=compression_opts,
                )
                logger.info("created dataset %s", self._dset_name)

        ndset = fh.get(self._number_dset_name)
        length = ndset.shape[0]
        ndset.resize(length + 1, axis=0)
        ndset[length] = int(evt_n)
        n = dset.shape[0]
        dset.resize(n + 1, axis=0)
        offsets = [n, *[0] * (dset.ndim - 1)]
        offsets[1] = 0  # ???
        dset.id.write_direct_chunk(offsets, acq.data["buffer"])
        logger.debug("wrote frame at offsets %s", offsets)
        return n

    def process_event(
        self,
        event: EventData,
        parameters: Parameters | None = None,
        tick: bool = False,
        *args: Any,
        **kwargs: Any,
    ):
        ret = {"worker_name": self.name}

        if self.stream_name in event.streams:
            acq = parse(event.streams[self.stream_name])
            if isinstance(acq, EigerLegacyHeader):
                meta_keys = [
                    "count_time",
                    "countrate_correction_applied",
                    "countrate_correction_count_cutoff",
                    "photon_energy",
                    "threshold_energy",
                    "flatfield_correction_applied",
                    "virtual_pixel_correction_applied",
                    "pixel_mask_applied",
                    "nimages",
                    "ntrigger",
                    "trigger_mode",
                ]
                meta_header = acq.appendix
                meta_info = {key: acq.info[key] for key in meta_keys}
                ret["header"] = self.open_file(meta_header, meta_info, parameters)
            elif isinstance(acq, EigerLegacyImage):
                frame_n = event.event_number - 1
                pos = self.write_frame(acq, frame_n)
                if pos is not None:
                    ret["frame"] = {"frame_number": frame_n, "position": pos}

        return ret


def finalize(self, *args, **kwargs):
    # close the file
    if self._fh is not None:
        self._fh.close()
