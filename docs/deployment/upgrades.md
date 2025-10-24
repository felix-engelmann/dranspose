# Version Upgrades

This document keeps track of new features and breaking changes which need to be applied to payloads and modifications to the external interface of the framework.

## 0.2.0

### Mapping Endpoints

The controller `/v1/api/mapping` endpoint is soon to be deprecated but still available. Please move to `/api/v1/sequence`

### Pickled Result

The reducer and replay no longer expose the `self.publish` data via pickle at `/api/v1/result/*`
To assure that data is fetchable via h5pyd, make sure the dictionary only has string keys and no "complex" python objects.

### Reducer timer()

The `timer()` function of the reducer now receives a copy of the analysis parameters as an argument. Make sure to update the signature of the function if you implement it.

### HDF5 dumper

A new 
def `dump(data, filename, lock)` function is available in `helpers.h5dump`. You can pass it your `self.publish` dictionary and it will dump its content, including the metadata attributes, to an HDF5 file.

## 0.1.1

This is considered the initial partially stable release against which several payloads have been developed.
