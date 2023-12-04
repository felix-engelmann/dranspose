from typing import Optional, Any

import numpy as np
from jsonpath_ng.ext.parser import ExtentedJsonPathParser  # type: ignore
from jsonpath_ng import DatumInContext, This  # type: ignore


class DefintionInvalid(Exception):
    pass


class Numpy(This):
    """Numpy slicer

    Concrete syntax is '`slice(numpy slice, multidim)`'
    """

    def _parse_slice(self, slice_str: str) -> Any | int | slice:
        if slice_str == "...":
            return Ellipsis
        elif slice_str.isdigit():
            return int(slice_str)
        else:
            return slice(*map(lambda x: int(x) if x else None, slice_str.split(":")))

    def __init__(self, method: Optional[str] = None) -> None:
        if not method:
            raise DefintionInvalid("method must be provided")
        method = method[len("numpy(") : -1]
        slice_objects = [self._parse_slice(s.strip()) for s in method.split(",")]
        if slice_objects is []:
            raise DefintionInvalid("%s is not valid" % method)
        self.slice = tuple(slice_objects)
        self.method = method

    def find(self, datum: DatumInContext) -> list[DatumInContext]:
        # print("datum got", datum)
        # datum = DatumInContext.wrap(datum)
        try:
            # value = datum.value[self.slice] #.split(self.char, self.max_split)[self.segment]
            datum = DatumInContext(datum.value[self.slice], path=self, context=datum)
        except Exception:
            return []
        return [datum]

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Numpy) and self.method == other.method

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.method)

    def __str__(self) -> str:
        return "`%s`" % self.method


class NumpyExtentedJsonPathParser(ExtentedJsonPathParser):
    """Custom LALR-parser for JsonPath"""

    def p_jsonpath_named_operator(self, p: list[Any]) -> None:
        "jsonpath : NAMED_OPERATOR"
        if p[1].startswith("numpy("):
            p[0] = Numpy(p[1])
        else:
            super(NumpyExtentedJsonPathParser, self).p_jsonpath_named_operator(p)
