import logging
from contextlib import nullcontext

import h5py


def _dump_dict(obj, grp):
    if isinstance(obj, dict):
        for key, val in obj.items():
            if not isinstance(key, str):
                logging.warning("unable to use non-string key: %s as group name", key)
                continue
            if key.endswith("_attrs"):
                continue
            if isinstance(val, dict):
                new_grp = grp.create_group(key)
                _dump_dict(val, new_grp)
            else:
                try:
                    grp.create_dataset(key, data=val)
                except Exception as e:
                    logging.warning("unable to dump %s: %s", key, e.__repr__())
        for key, val in obj.items():
            if key.endswith("_attrs"):
                if key[:-6] in obj:
                    for att, attval in val.items():
                        grp[key[:-6]].attrs[att] = attval


def dump(data, filename, lock=None):
    lock = lock or nullcontext()
    with lock:
        group = h5py.File(filename, "w")
        _dump_dict(data, group)
        if "_attrs" in data:
            for att, attval in data["_attrs"].items():
                group.attrs[att] = attval
