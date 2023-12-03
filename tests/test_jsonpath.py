import numpy as np

from dranspose.helpers.jsonpath_slice_ext import NumpyExtentedJsonPathParser


def test_jsonpath() -> None:
    res = NumpyExtentedJsonPathParser(debug=False).parse("map, meta")
    print(res.__repr__())
    dat = {"map": 1, "meta": [1, 2, 3]}
    result = res.find(dat)
    print("or result", [r.value for r in result])
    res = NumpyExtentedJsonPathParser(debug=False).parse("map.`numpy(:,6:8,5)`")
    print(res.__repr__())

    data = {"map": np.linspace(0, 999, 1000).reshape((10, 10, 10)), "meta": [1, 2, 3]}
    # print(data)

    result = [r.value for r in res.find(data)]
    print(result[0])
    arr = np.array(
        [
            [65.0, 75.0],
            [165.0, 175.0],
            [265.0, 275.0],
            [365.0, 375.0],
            [465.0, 475.0],
            [565.0, 575.0],
            [665.0, 675.0],
            [765.0, 775.0],
            [865.0, 875.0],
            [965.0, 975.0],
        ]
    )
    print(arr)
    assert arr.tolist() == result[0].tolist()
