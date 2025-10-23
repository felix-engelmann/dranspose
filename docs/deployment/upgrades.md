# Version Upgrades

This document keeps track of breaking changes which need to be applied to payloads.

## 0.2.0

### Mapping Endpoints

The controller `/v1/api/mapping` endpoint is soon to be deprecated but still available. Please move to `/api/v1/sequence`

### Pickled Result

The reducer and replay no longer expose the `self.publish` data via pickle at `/api/v1/result/*`
To assure that data is fetchable via h5pyd, make sure the dictionary only has string keys and no "complex" python objects.

## 0.1.1

This is considered the initial partially stable release against which several payloads have been developed.
