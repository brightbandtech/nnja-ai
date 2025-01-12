# Dataset Documentation

## Overview

The original [NOAA NASA Joint Archive](https://psl.noaa.gov/data/nnja_obs/) (NNJA) has data organized by "sensor" and "source".
For a given sensor/source, there are one or more message types.
Often this mapping is 1:1:1 (one message per source, one source per sensor), however a sensor can contain multiple sources and a source can contain multiple messages.
For example, the SEVIRI sensor contains two sources: `sevscr` for clear-sky radiances (message type NC021043), and `sevasr` for all-sky radiance (message type NC021042).
Since the message specifies the underlying data schema, NNJA-AI datasets correspond to individual messages.

## Data Structure

The data is tabular (with columns for datetime, lat, lon, and many data/metadata columns) and stored as hive-partitioned parquets, so the intended use of this library is to find data and quickly load to a familiar tabular data analysis tool (pandas, dask, polars) for any further work.

## Datasets

### AMSU-A

- Sensor: `amsua`
- Source: `1bamua`
- Message: `NC021023`
- Dates processed: `2021-August 2024`

This dataset contains the AMSU-A Level 1B brightness temperatures from both the METOP-XX and NOAA-XX polar-orbiting satellite series.
The primary data packet is the brightness temperature retrievals for 15 channels (TMBR_00001 - TMBR_00015).
Its main purpose is to constrain atmospheric temperatures.
#### Additional Resources

- [OSCAR Instrument detail page](https://space.oscar.wmo.int/instruments/view/amsu_a)
- [Eumetsat data processing documentation](https://user.eumetsat.int/s3/eup-strapi-media/pdf_ten_990005_eps_amsal1_pgs_6ccda24e33.pdf)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/amsua/1bamua/)


### ATMS

- Sensor: `atms`
- Source: `atms`
- Message: `NC021203`
- Dates processed: `2021-August 2024`


This dataset contains the Advanced Technology Microwave Sounder (ATMS) brightness temperatures from multiple polar-orbiting satellites.
The primary data packed is the brightness temperature retrievals for 22 channels (TMBR_00001 - TMBR_00022).
Its main purpose is to constrain atmospheric temperatures and humidity.
#### Additional Resources

- [OSCAR Instrument detail page](https://space.oscar.wmo.int/instruments/view/atms)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/atms/atms/)


### MHS

- Sensor: `mhs`
- Source: `1bmhs`
- Message: `NC021027`
- Dates processed: `2021-August 2024`

This dataset contains the Microwave Humidity Sounder (MHS) brightness temperatures from both the METOP-XX and NOAA-XX polar-orbiting satellite series.
The primary data packet is the brightness temperature retrievals for 5 channels (TMBR_00001 - TMBR_00005).
Its main purpose is to measure atmospheric humidity and provide data for weather forecasting and climate monitoring.
#### Additional Resources

- [OSCAR Instrument detail page](https://space.oscar.wmo.int/instruments/view/mhs)
- [Eumetsat data processing documentation](https://user.eumetsat.int/s3/eup-strapi-media/pdf_ten_97229_eps_mhs_pfs_2069b45efc.pdf)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/mhs/1bmhs/)
