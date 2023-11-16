# dranspose

dranspose is a framework to perform a distributed tanspose from multiple streams of partial events into parallel workers with full events.

This is useful e.g. for assembling image frames which are captures separately.


## testing

To get debug logs for the tests which are useful when debugging use

    pytest --log-cli-level=DEBUG 