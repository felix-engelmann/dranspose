# dranspose

dranspose is a framework to perform a distributed transpose from multiple streams of partial events into parallel workers with full events.

This is useful e.g. for assembling image frames which are captured by separate detectors.

* [Documentation](https://felix-engelmann.github.io/dranspose/)

## Installation

To install the package run

    pip install dranspose

## Usage

The `dranspose` cli wrapper allows to start different parts of the system or run them combined

## Testing

To get debug logs for the tests which are useful when debugging use

    pytest --log-cli-level=DEBUG 