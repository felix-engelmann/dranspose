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
from dranspose.protocol import StreamName, ParameterName, WorkParameter, WorkerState
from dranspose.data.eiger_legacy import (
    EigerLegacyEnd,
    EigerLegacyImage,
    EigerLegacyHeader,
)
from dranspose.middlewares.eiger_legacy import parse

logger = logging.getLogger(__name__)


class WriterWorker:
    def __init__(self, state: WorkerState, **kwargs: Any) -> None:
        self._fh: Optional[h5py.File] = None
        self.name = state.name
        # do not change stream name once established
        # self.stream_name = StreamName(parameters[("stream_name")].data)
        self.stream_name = StreamName("eiger")
        self._dset_name = f"/entry/instrument/{self.stream_name}/data"

    @staticmethod
    def describe_parameters() -> list[ParameterType]:
        params = [
            StrParameter(name=ParameterName("stream_name"), default="eiger"),
        ]
        return params

    def save_dict_to_h5(self, data: dict[str, Any], group: h5py.Group) -> None:
        for key, value in data.items():
            if isinstance(value, dict):
                ng = group.create_group(key)
                self.save_dict_to_h5(value, ng)
            else:
                group.create_dataset(key, data=value)

    def open_file(self, meta_header: dict, meta_info: dict):
        logger.error("open_file args: %s, %s", meta_header, meta_info)
        # here is what the worker does
        # queue.put([meta_header, meta_info])
        # this is how the writer treats it:
        # parts = writer_queue.get()
        # header = parts[0]
        # self._handle_start(header, parts, worker_queue)
        ret = {"name": self.name}
        filename = meta_header.get("filename", "")
        saveraw = meta_info.get("save_raw", True)
        logger.info("Original parameters %s %s", filename, saveraw)
        # framesperfile = meta_info.get("nframes_per_file", 0)
        # pre_generate_files = meta_info.get("pre_generate_files", False)
        # expected_data = meta_info.get("expected_data", {})
        # _nframes_total = expected_data.get("nframes", None)
        # for key in ["shape", "type", "nframes"]:
        #     pre_generate_files = (
        #         pre_generate_files and key in expected_data
        #     )
        # _nframes_overlay = meta_info.get("nframes_overlay", 0)
        if filename and filename.startswith("/data") and saveraw:
            base, ext = os.path.splitext(filename)
            filename = f"{base}_{self.name}{ext}"
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
            logger.info("no file opened. filename: %s save_raw: %s", filename, saveraw)
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

    def process_event(
        self,
        event: EventData,
        parameters: dict[ParameterName, WorkParameter] | None = None,
        tick: bool = False,
        *args: Any,
        **kwargs: Any,
    ):
        ret = {}

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
                print("open_file args: %s, %s", meta_header, meta_info)
                ret["header"] = self.open_file(meta_header, meta_info)
            elif isinstance(acq, EigerLegacyImage):
                acq.buffer = b"some data removed"
            elif isinstance(acq, EigerLegacyEnd):
                if self._fh is not None:
                    self._fh.close()
            logger.info("parsed packet %s", acq)

        return ret
