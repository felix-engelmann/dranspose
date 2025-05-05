# Live Viewers

The reducer publishes data available for live viewing or to influence the scanning.

To retrieve the data, the reducer exposes an HTTP interface which follows the [hdf rest api](https://github.com/HDFGroup/hdf-rest-api)

This allows direct viewing with [h5pyd](https://github.com/HDFGroup/h5pyd) compatible software, such as [silx >= 2.2.0](https://github.com/silx-kit/silx)


!!! Example
    ```python
    import h5pyd

    f = h5pyd.File("http://<reducer>:<port>/", "r")
    data = f["map"][:,4:8,3]
    ```

    sets data to the `publish["map"][:,4:8,3]` element of the numpy array in "map" and otherwise behaves like an hdf5 file.
