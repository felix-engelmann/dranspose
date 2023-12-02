# Live Viewers

The reducer publishes data available for live viewing or to influence the scanning.

To retrieve the data, the reducer exposes a HTTP interface. All data is available at

```
/api/v1/result/
```

and returns pickle objects.

However, if you are only interested in a subset of data for your specific live viewer, 
use [JSONpath](https://pypi.org/project/jsonpath-ng/#description) to select the specific data you need. To handle large numpy arrays better,
there is a special filter `numpy()` which takes numpy slicing syntax.

!!! Example
    ```jsonpath
    /api/v1/result/$.map.`numpy(:,4:8,3)`
    ```
    returns the `publish["map"][:,4:8,3]` element of the numpy array in "map"
