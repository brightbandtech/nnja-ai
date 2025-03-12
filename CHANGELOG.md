# Changelog

## Unreleased
### Added
- Documentation for the following datasets: ADPSFC, geostationary satellites
- Created FAQ page and understanding-the-data for more details
- Support for anonymous credentials in GCS access

### Changed
- Updated backend_kwargs handling in pandas load_dataset
- Relaxed Python version requirements (from 3.12+ to 3.10+)
- Tweaked how format strings for dimensions are parsed (using .format() instead of more restrictive f-string formatting). This is a breaking change since we are updating the dataset jsons to match the new format.

### Fixed
- Corrected variable naming from `engine` to `backend` in example notebook


## [0.1.0] - 2024-11-01
### Added
- Initial release of the `nnja` Python SDK.
- Support for loading and interacting with NNJA datasets.
- Integration with Google Cloud Storage using `fsspec` and `gcsfs`.
- JSON schema validation for dataset metadata.
- Example notebook demonstrating basic usage.
