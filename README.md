# nnja-ai
This is the companion Python SDK to the [Brightband](https://www.brightband.com/) AI-ready reprocessing of the [NOAA NASA Joint Archive](https://psl.noaa.gov/data/nnja_obs/) (NNJA).
It is meant to serve as a helpful interface between a user and the underlying NNJA datasets (which currently consist of parquet files on [GCS](https://console.cloud.google.com/storage/browser/nnja-ai)).

## Background
The NNJA archive project is a curated archive of Earth system data from 1979 to present.
This data represents a rich trove of observational data for use in AI weather modelling, however the archival format in which the data is originally available (BUFR) is cumbersome to work with.
In [partnership with NOAA](https://techpartnerships.noaa.gov/tpo_partnership/making-observation-data-ai-ready/), Brightband is processing that data to make it more accessible to the community.

## Data
NNJA datasets are organized by sensor/source (e.g. all-sky radiances from the GOES ABI).
The list of all NNJA datasets can be found on the [NNJA project page](https://psl.noaa.gov/data/nnja_obs/#data-sources), while the subset that is currently found in the NNJA-AI archive can be found [here](docs/datasets.md) or by exploring the data catalog (this will be be expanding rapidly).

The data is tabular in structure (with columns for date, lat, lon, and many data/metadata columns) and stored as hive-partitioned parquets, so the intended use of this library is to find data and quickly load to a familiar tabular data analysis tool (pandas, dask, polars) for any further work.


## Getting Started

To install this package directly from the GitHub repository, you can use the following `pip` command:

```sh
pip install git+https://github.com/brightbandtech/nnja-ai.git
```
You can find an example notebook [here](example_notebooks/basic_dataset_example.ipynb) showing the basics of opening the data catalog, finding a dataset, subsetting, and finally loading the data to pandas.
Though to get started, you can open the data catalog like so:

```python
from nnja import DataCatalog
catalog = DataCatalog()
print("datasets in catalog:", catalog.list_datasets())
```

## How to Cite
If you use this library or the Brightband reprocessed NNJA data, please cite it using the following DOI:

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1234567.svg)](insertlinkhere)

Additionally, please follow the citation guidance on the [NNJA project page](https://psl.noaa.gov/data/nnja_obs/#cite
).

The NNJA-AI data is distributed with the same license as the original NNJA data, [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.en)
