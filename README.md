[![GitHub Actions - CI](https://github.com/brightbandtech/nnja-ai/actions/workflows/ci.yaml/badge.svg)](https://github.com/brightbandtech/nnja-ai/actions/workflows/ci.yaml)
[![ReadTheDocs - Status](https://app.readthedocs.org/projects/nnja-ai/badge/?version=stable)](https://nnja-ai.readthedocs.io/en/stable/)
[![PyPI - Latest](https://img.shields.io/pypi/v/nnja-ai
)](https://pypi.org/p/nnja-ai)
[![zenodo](https://zenodo.org/badge/899259654.svg)](https://doi.org/10.5281/zenodo.14633508)

---

# nnja-ai: multi-modal, AI-ready weather observations

This is the companion Python SDK to the [Brightband](https://www.brightband.com/) AI-ready reprocessing of the [NOAA NASA Joint Archive](https://psl.noaa.gov/data/nnja_obs/) (NNJA).
It is meant to serve as a helpful interface between a user and the underlying NNJA datasets (which currently consist of parquet files on [GCS](https://console.cloud.google.com/storage/browser/nnja-ai)).

The V1 release of the NNJA-AI dataset and SDK represents a major increment in availability of NNJA data, with ~50 TiB of observations made available in parquet form along with a data catalog and code examples in this SDK.

## Background
The NNJA archive project is a curated archive of Earth system data from 1979 to present.
This data represents a rich trove of observational data for use in AI weather modelling, however the archival format in which the data is originally available (BUFR) is cumbersome to work with.
In [partnership with NOAA](https://techpartnerships.noaa.gov/tpo_partnership/making-observation-data-ai-ready/), Brightband is processing that data to make it more accessible to the community.

## Data
NNJA datasets are organized by sensor/source (e.g. all-sky radiances from the GOES ABI).
The list of all NNJA datasets can be found on the [NNJA project page](https://psl.noaa.gov/data/nnja_obs/#data-sources), while the subset that is currently found in the NNJA-AI archive can be found [here](docs/datasets.md) or by exploring the data catalog (this will be be expanding rapidly).

## Getting Started

To install this package directly from the GitHub repository, you can use the following `pip` command:

```sh
pip install git+https://github.com/brightbandtech/nnja-ai.git
```
You can find an example notebook [here](example_notebooks/basic_dataset_example.ipynb) showing the basics of opening the data catalog, finding a dataset, subsetting, and finally loading the data to pandas.
Though to get started, you can open the data catalog like so:

```python
from nnja_ai import DataCatalog
catalog = DataCatalog()
print("datasets in catalog:", catalog.list_datasets())
```

```
datasets in catalog:

['amsua-1bamua-NC021023',
 'atms-atms-NC021203',
 'mhs-1bmhs-NC021027',
 'cris-crisf4-NC021206',
 ...]
```

## How to Cite
If you use this library or the Brightband reprocessed NNJA data, please cite it using the following DOI:

[![DOI](https://zenodo.org/badge/899259654.svg)](https://doi.org/10.5281/zenodo.14633508)

Additionally, please follow the citation guidance on the [NNJA project page](https://psl.noaa.gov/data/nnja_obs/#cite
).

The NNJA-AI data is distributed with the same license as the original NNJA data, [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.en).
