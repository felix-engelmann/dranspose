# dranspose

Dranspose introduces a novel approach to experimental feedback by enabling direct intervention during scanning. 
Rather than waiting for a scan to complete, this software allows real-time interruption of suboptimal scans, 
enhancing efficiency and reducing the likelihood of processing flawed data. 
Dranspose's capability to intervene during scanning represents a valuable tool for researchers seeking immediate control over experimental outcomes, 
promoting a more agile and adaptive research process.


## Getting started

The easiest way to test dranspose is to install the python package

    pip install dranspose

To get a feel of building an analysis, please follow the [tutorial](tutorials/analysis.md).

### Conda

Dranspose is also available on [conda-forge](https://anaconda.org/conda-forge/dranspose) and is installed with

     conda install conda-forge::dranspose 


## CLI

The packet provides a cli `dranspose` to run the components separately or all combined. 
To run the full distributed system, a `redis` server is required. A convenient way is to use the [docker image](https://hub.docker.com/r/redis/redis-stack)

!!! info "NOBUGS 2024"
    There is a [conference talk](https://indico.esrf.fr/event/114/contributions/750/) with a new overview and performance measurements.

    [Slides](nobugs-2024-09-25/main.pdf){ .md-button }


!!! info "Presentation"
    There is a seminar talk with a general overview available. Some details might be out of date.

    [Slides](seminar-2023-11-30/main.pdf){ .md-button }

!!! info "Poster"
    There is a poster with developer information.

    [Poster](poster-2024-01-15/main.pdf){ .md-button }

## Applications

If you are looking for help with developing an application on top of dranspose, have a look at the [application section](applications/overview.md)

### Example Payloads

There are some [examples](https://github.com/orgs/maxiv-science/repositories?q=drp) of analysis payloads available for beamlines at Max IV.
They provide an overview of possible live calculations possible with dranspose.


