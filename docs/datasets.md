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


### IASI

- Sensor: `iasi`
- Source: `mtiasi`
- Message: `NC021241`
- Dates processed: `2021-January 2024`

This dataset contains the Infrared Atmospheric Sounding Interferometer (IASI) radiances from the METOP-XX polar-orbiting satellite series.
The primary data packet (`SCRA_XXXXX`, 616 columns total) includes radiance measurements across a wide spectral range, providing detailed information on atmospheric temperature, humidity, and trace gases (including ozone)

Note on data processing: IASI raw data in BUFR format is stored with scaling factors (list column `IASIL1CB`).
These have already been applied to the radiances in the NNJA-AI version of the dataset; no further scaling is needed.

#### Additional Resources

- [OSCAR Instrument detail page](https://space.oscar.wmo.int/instruments/view/iasi)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/iasi/mtiasi/)


### CrIS

- Sensor: `cris`
- Source: `crisf4`
- Message: `NC021206`
- Dates processed: `2021-August 2024`

This dataset contains the Cross-track Infrared Sounder (CrIS) radiances from the NOAA-20 and Suomi NPP satellites.
The primary data packet (columns SRAD01_XXXXX, 431 in total) includes radiance measurements across a large spectral range, providing information about temperature and humidity profiles, as well as ozone.

#### Additional Resources

- [OSCAR Instrument detail page](https://space.oscar.wmo.int/instruments/view/cris)
- [EUMETSAT product page](https://navigator.eumetsat.int/product/EO:EUM:DAT:MULT:EARS-CRIS/print)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/cris/crisf4/)


### ADPSFC

These datasets contain NCEP ADP Global Surface Observational Weather data. There are currently 4 ADPSFC datasets, all containing surface station observations from different data sources. These are part of the "conv" collection of sources, representing conventional observation platforms (surface stations, buoys, weather balloons, etc.). The surface station datasets can be differentiated by their message ID, which correspond to the data channel through with they are received.

While generally these dataset all contain common data (e.g. temperature, dew point, wind speeds) and data descriptors (latitude, longitude, station elevation), the naming of these variables will differ slightly between messages (e.g. dew point may be `TEMHUMDA.TMDP` vs `MPSQ1.TMDP`). These datasets have many columns to parse though, so a useful snippet to pare down the data according to our subjective classification of variables is as follows:

```
dataset = catalog["conv-adpsfc-NC000001"]  # using NC000001 as example
primary_vars = [k for k,v in dataset.variables.items() if v.category in ['primary data','primary descriptors']]
dataset = dataset.sel(variables=primary_vars)
dataset.variables
```

Some fields have complex meanings; be sure to read [Understanding the Data](/docs/understanding-the-data.md).

#### ADPSFC - Synoptic Fixed Land (from WMO SYNOP bulletins)

- Sensor: `conv`
- Source: `adpsfc`
- Message: `NC000001`
- Dates processed: `2021-August 2024`

 Surface data from fixed land stations, received through WMO SYNOP bulletins. This dataset and [NC000101](/docs/datasets.md#adpsfc---synoptic-fixed-land-originally-in-bufr) come from the same sources; the WMO SYNOP bulletins are legacy reporting messages while the BUFR messages are from stations that have migrated to the newer report type. At the current time, most stations have migrated to BUFR.


#### ADPSFC - Synoptic Mobile Land (from WMO SYNOP MOBIL bulletins)

- Sensor: `conv`
- Source: `adpsfc`
- Message: `NC000002`
- Dates processed: `2021-August 2024`

Surface data from mobile land stations, received through WMO SYNOP bulletins. This dataset and NC000102 (not currently processed) come from the same sources; the WMO SYNOP bulletins are legacy reporting messages while the BUFR messages are from stations that have migrated to the newer report type. Note that the stations in this dataset mostly originate from South Asia.

#### ADPSFC - Aviation (METAR/SPECI)

- Sensor: `conv`
- Source: `adpsfc`
- Message: `NC000007`
- Dates processed: `2021-August 2024`

Surface data from aviation weather reports (METAR/SPECI). This dataset contains observations from airports and other aviation facilities, providing information on temperature, dew point, wind speed and direction, visibility, and other meteorological variables critical for aviation operations.

#### ADPSFC - Synoptic Fixed Land (originally in BUFR)

- Sensor: `conv`
- Source: `adpsfc`
- Message: `NC000101`
- Dates processed: `2021-August 2024`

Surface data from fixed land stations, originally received in BUFR format. This dataset and [NC000001](/docs/datasets.md#adpsfc---synoptic-fixed-land-from-wmo-synop-bulletins) come from the same sources; the WMO SYNOP bulletins are legacy reporting messages while the BUFR messages are from stations that have migrated to the newer report type. At the current time, most stations have migrated to BUFR.

#### Additional Resources

- [SYNOP Codes](https://en.wikipedia.org/wiki/SYNOP)
- [Understanding METAR/SPECI](https://aviationweather.gov/help/data/#metar)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/conv/convbufr/adpsfc/)


### ADPUPA - Fixed Land Upper-air Rawinsonde data

- Sensor: `conv`
- Source: `adpupa`
- Message: `NC002001`
- Dates processsed: `2021-August 2024`

Upper-air rawinsonde (radiosonde) data. This is a sparse dataset with coverage over land only. The main data packet is temperature, dew point temperature, wind speed, and wind direction, and geopotential, on pressure levels (columns with prefixes, in order, `TMDB_`, `TMDP_`, `WSPD_`, `WDIR_`, `GP10_`). These data are originally packaged into the `UARLV` column, however their structure does not lend itself well to flattening. The UPA profile plotting [example notebook](/example_notebooks/adpupa_profile_example.ipynb) demonstrates some of the nuances of accessing this data.

#### Additional Resources
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/conv/convbufr/adpupa/)




### Himawari Advanced Himawari Imager (AHI) Clear-Sky Radiance

- Sensor: `geo`
- Source: `ahicsr`
- Message: `NC021044`
- Dates processed: `March 2021-August 2024`

This dataset contains clear-sky radiance measurements from the Advanced Himawari Imager (AHI) instrument aboard the JMA Himawari geostationary satellites. The data is processed to include only clear-sky conditions. The primary data packet consists of brightness temperature measurements (TMBRST) for 10 channels, with channels 11 and 12 currently not populated, nor are the channel band widths (SCBW) and cloud type (CLTP) populated for all channels.

#### Additional Resources

- [OSCAR Instrument Detail Page](https://space.oscar.wmo.int/instruments/view/ahi)
- [JMA Himawari Overview](https://www.data.jma.go.jp/mscweb/en/himawari89/space_segment/spsg_ahi.html)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/geo/ahicsr/)

### GOES All-Sky Radiances

- Sensor: `geo`
- Source: `gsrasr`
- Message: `NC021045`
- Dates processed: `March 2021-August 2024`

This dataset contains all-sky radiance measurements from the Advanced Baseline Imager (ABI) aboard GOES satellites. The data includes radiance measurements under all atmospheric conditions, providing comprehensive coverage of the Americas and surrounding oceanic regions. The primary data packet includes brightness temperature measurements for 10 channels under various conditions:
- All-sky conditions (TMBRST_allsky)
- Clear-sky conditions (TMBRST_clearsky)
- All-cloud conditions (TMBRST_allcloud)

While the dataset does have cloud-height specific measurements (TMBRST_lowcloud, TMBRST_midcloud, TMBRST_highcloud), they are currently not populated.
#### Additional Resources

- [OSCAR Instrument Detail Page](https://space.oscar.wmo.int/instruments/view/abi)
- [GOES ABI Overview](https://www.goes-r.gov/spacesegment/abi.html)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/geo/gsrasr/)

### GOES Clear-Sky Radiances

- Sensor: `geo`
- Source: `gsrcsr`
- Message: `NC021046`
- Dates processed: `March 2021-August 2024`

This dataset contains clear-sky radiance measurements from the Advanced Baseline Imager (ABI) aboard GOES satellites. The data is specifically filtered to include only measurements taken under clear-sky conditions, making it particularly valuable for applications requiring unobstructed atmospheric measurements and radiative transfer modeling. The primary data packet consists of brightness temperature measurements (TMBRST) for 10 channels.

#### Additional Resources

- [OSCAR Instrument Detail Page](https://space.oscar.wmo.int/instruments/view/abi)
- [GOES ABI Overview](https://www.goes-r.gov/spacesegment/abi.html)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/geo/gsrcsr/)

### SEVIRI All-Sky Radiances

- Sensor: `seviri`
- Source: `sevasr`
- Message: `NC021042`
- Dates processed: `March 2022-August 2024`

This dataset contains all-sky radiance measurements from the Spinning Enhanced Visible and InfraRed Imager (SEVIRI) instrument aboard Meteosat Second Generation (MSG) satellites. The data includes radiance measurements under all atmospheric conditions, providing comprehensive coverage over Europe, Africa, and surrounding regions. The primary data packet includes brightness temperature measurements for 11 channels (visible/near-IR channels 1-3 currently not populated) under various conditions:
- All-sky conditions (TMBRST_allsky)
- Clear-sky conditions (TMBRST_clearsky)
- All-cloud conditions (TMBRST_allcloud)
- Cloud-type specific conditions (high/mid/low cloud, TMBRST_highcloud, etc.)

Each measurement type includes corresponding standard deviation fields (SDTB). Some cloud-type specific measurements have partial coverage.

#### Additional Resources

- [OSCAR Instrument Detail Page](https://space.oscar.wmo.int/instruments/view/seviri)
- [BUFR source data on AWS](https://noaa-reanalyses-pds.s3.amazonaws.com/index.html#observations/reanalysis/seviri/sevasr/)
