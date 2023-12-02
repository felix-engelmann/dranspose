import numpy as np
from jsonpath_ng.ext.parser import ExtentedJsonPathParser
from jsonpath_ng import DatumInContext, This


class DefintionInvalid(Exception):
    pass


class Numpy(This):
    """Numpy slicer

    Concrete syntax is '`slice(numpy slice, multidim)`'
    """

    def _parse_slice(self, slice_str: str):
        if slice_str == "...":
            return Ellipsis
        elif slice_str.isdigit():
            return int(slice_str)
        else:
            return slice(*map(lambda x: int(x) if x else None, slice_str.split(":")))

    def __init__(self, method=None):
        print("got arguments", method)
        # m = SPLIT.match(method)
        method = method[len("numpy(") : -1]
        slice_objects = [self._parse_slice(s.strip()) for s in method.split(",")]
        if slice_objects is []:
            raise DefintionInvalid("%s is not valid" % method)
        self.slice = tuple(slice_objects)
        self.method = method

    def find(self, datum):
        # print("datum got", datum)
        # datum = DatumInContext.wrap(datum)
        try:
            # value = datum.value[self.slice] #.split(self.char, self.max_split)[self.segment]
            datum = DatumInContext(datum.value[self.slice], path=self, context=datum)
        except Exception:
            return []
        return [datum]

    def __eq__(self, other):
        return isinstance(other, Numpy) and self.method == other.method

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.method)

    def __str__(self):
        return "`%s`" % self.method


class NumpyExtentedJsonPathParser(ExtentedJsonPathParser):
    """Custom LALR-parser for JsonPath"""

    def p_jsonpath_named_operator(self, p):
        "jsonpath : NAMED_OPERATOR"
        if p[1].startswith("numpy("):
            p[0] = Numpy(p[1])
        else:
            super(NumpyExtentedJsonPathParser, self).p_jsonpath_named_operator(p)


if __name__ == "__main__":
    res = NumpyExtentedJsonPathParser(debug=False).parse("map, meta")
    print(res.__repr__())
    dat = {"map": 1, "meta": [1, 2, 3]}
    result = res.find(dat)
    print("or result", [r.value for r in result])
    res = NumpyExtentedJsonPathParser(debug=False).parse("meta,map.`numpy(:,6:8,5)`")
    print(res.__repr__())

    data = {"map": np.linspace(0, 999, 1000).reshape((10, 10, 10)), "meta": [1, 2, 3]}
    # print(data)

    result = [r.value for r in res.find(data)]
    print(result)
