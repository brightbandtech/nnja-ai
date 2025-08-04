# Frequently Asked Questions

- [What is the status of this archive?](#what-is-the-status-of-this-archive)
- [What datasets do you have?](#what-datasets-do-you-have)
- [What format is the data in and where is it stored?](#what-format-is-the-data-in-and-where-is-it-stored)
- [Why is are some columns still a structured field?](#why-is-are-some-columns-still-a-structured-field)
- [Why is this entire column missing data?](#why-is-this-entire-column-missing-data)
- [Why didn't you use zarr instead of parquet?](#why-didn-t-you-use-zarr-instead-of-parquet)
- [There are too many columns! How do I filter them?](#there-are-too-many-columns-how-do-i-filter-them)
- [Will you keep the datasets current with the most recent data?](#will-you-keep-the-datasets-current-with-the-most-recent-data)
- [How do I get in touch with you?](#how-do-i-get-in-touch-with-you)

## What is the status of this archive?
The NNJA-AI v1 release contains all the data currently processed to parquet form at time of writing (Aug 2025). Data processing from the NNJA archive is ongoing at Brightband. New datasets will be added periodically.

## What datasets do you have?
All datasets currently available are listed in the [datasets documentation](/docs/datasets.md).
We are working to add more datasets to the archive, and will update the documentation as we add more datasets.

## What format is the data in and where is it stored?
The data is stored on GCS in parquets with partitions for each day.
See the example notebook [here](/example_notebooks/basic_dataset_example.ipynb) for a guide on how to access the data.
If you prefer to bypass this SDK, you can currently find the v1 datasets here:
`gs://nnja-ai/data/v1/` for direct access to the parquet files.

## What have you done to process the NNJA BUFR files?
1) convert from BUFR to AVRO, preserving all the structure of the original BUFR messages (nested types, etc.).
2) 'flatten' the complex columns (array, struct) into simple scalar columns.
3) combined 6-hourly files into daily Parquet partitions based on the observation timestamp.

## Why are some columns still a structured field?
The original BUFR data is highly structured, with multiple levels of nested data.
While we have flattened the data as much as possible, there are some cases where the data is still structured.
This is either because the original data could not be flattened
(this is the case for fields which are variable-length lists, such as the upper air significant level data),
or because the data is not worth flattening (e.g. because all subcolumns are null-valued).

## Why didn't you use zarr instead of parquet?
We considered using zarr as the underlying storage format, but decided to use parquet for a few reasons:
- The original BUFR data is inherently tabular, and parquet is a good format for tabular data.
- Generally the data is one-dimensional (time), and zarr's strength is in multi-dimensional data.
There are some datasets that have some additional dimensions (e.g. channel for satellite data),
but we decided to stick with one format for simplicity.
- There are many tools and libraries that support parquet (e.g. pandas, polars, dask, BigQuery, etc.)

## There are too many columns! How do I filter them?
We have classified the columns into the following categories, based on our understanding of the data:
`primary data`, `primary descriptors`, `secondary data`, `secondary descriptors`.
You can quickly filter the columns with a snippet like the following:
```
dataset = catalog[DATASET_NAME]
primary_vars = [k for k,v in dataset.variables.items() if v.category in ['primary data','primary descriptors']]
dataset = dataset.sel(variables=primary_vars)
```

## What are code and flag table variables?
Some variables are encoded as a code table or flag table.
We're working on adding a helper utility to incorporate these into the archive and decode them,
but in the meantime you can use the Variable.info() method to view the code table or flag table to link to the NOAA code table page, or look them up directly here: https://www.nco.ncep.noaa.gov/sib/jeff/CodeFlag_0_STDv31_LOC7.html

```python
catalog = DataCatalog()
ds = catalog['seviri-sevasr-NC021042']
ds.variables['SIDENSEQ.SIDGRSEQ.SAID'].extra_metadata
```

```console
{'units': 'Code table',
 'code table': '001007',
 'code table link': 'https://www.nco.ncep.noaa.gov/sib/jeff/CodeFlag_0_STDv31_LOC7.html#001007'}
```


## Why is this or that entire column missing data?
We included all data fields from the original BUFR data.
Some data fields were not populated in the original data (e.g. GOES warm channels are not present in the ABI data).
If the [datasets documentation](/docs/datasets.md) does not mention a missing field that you think should be present,
please raise an issue on [GitHub](https://github.com/brightbandtech/nnja-ai/issues)
and we will check with the data providers and update the documentation (or if you check, please let us know!).

## Will you keep the datasets current with the most recent data?

The original NNJA archive is updated periodically. After we finalize a stable version of the AI archive,
we aim to maintain the archive up to date with the most recent data, schedule to be determined.

## How do I get in touch with you?

Feel free to raise an issue on [GitHub](https://github.com/brightbandtech/nnja-ai/issues)
or contact us at [hello@brightband.com](mailto:hello@brightband.com) with questions, suggestions, or feedback.
