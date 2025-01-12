import fsspec
import json
from typing import Literal, List, Union, Optional, TYPE_CHECKING
import pandas as pd
import logging
import jsonschema
import google.auth
from google.auth.transport.requests import Request

from nnja.exceptions import InvalidPartitionKeyError

if TYPE_CHECKING:
    import polars as pl
    import dask.dataframe as dd

VALID_TIME_INDEX = ["OBS_DATE", "OBS_HOUR"]
VALID_PARTITION_KEYS = ["OBS_DATE", "OBS_HOUR", "MSG_TYPE"]
USE_ANON_CREDENTIALS = True
if USE_ANON_CREDENTIALS:
    auth_args = {"token": "anon"}
else:
    auth_args = {}

logger = logging.getLogger(__name__)

Backend = Literal["pandas", "polars", "dask"]


def _check_authentication() -> bool:
    """Convenience function to check if the user is authenticated with GCS.

    This does not handle authentication, only checks if the user is authenticated.
    If the USE_ANON_CREDENTIALS flag is set, this function will always return True.
    First checks if the user has default credentials, and if not, attempts to refresh them.
    """
    if USE_ANON_CREDENTIALS:
        logger.info("Using anonymous credentials for GCS.")
        return True
    try:
        credentials, project = google.auth.default()

        if not credentials.valid:
            credentials.refresh(Request())

        logger.info("Authenticated with GCS.")
        return True
    except (
        google.auth.exceptions.DefaultCredentialsError,
        google.auth.exceptions.RefreshError,
    ):
        logger.error(
            "Authentication failed. Please run `gcloud auth application-default login` to reauthenticate."
        )
        return False


def read_json(json_uri: str, schema_path: Optional[str] = None) -> dict:
    """Read and validate a JSON file from a URI.

    Supports local and cloud storage URIs. If a JSON schema path is provided,
    the JSON file will be validated against the schema.

    Args:
        json_uri: URI pointing to the JSON file.
        schema_path: Path to the JSON schema file for validation.

    Returns:
        dict: The loaded JSON data.
    """
    with fsspec.open(json_uri, mode="r", **auth_args) as f:
        data = json.load(f)
    if schema_path:
        with fsspec.open(schema_path, mode="r") as f:
            schema = json.load(f)
        jsonschema.validate(data, schema)
        logger.debug("JSON file %s validated against schema %s", json_uri, schema_path)
    return data


def load_parquet(
    parquet_uris: List[str],
    columns: List[str],
    backend: Backend = "pandas",
    **backend_kwargs,
) -> Union["pd.DataFrame", "pl.LazyFrame", "dd.DataFrame"]:
    """Load parquet files using the specified backend; lazy if supported by the backend.

    With the current implementation, polars and dask will load lazily and preserve any
    hive partitions + columns, while pandas will load eagerly and concatenate the dataframes.

    Args:
        parquet_uris: List of URIs pointing to the parquet files.
        columns: List of columns to load from the parquet files.
        backend: Backend to use for loading the parquet files. Valid options are "pandas", "polars", and "dask".
                 Default is "pandas".
        **backend_kwargs: Additional keyword arguments to pass to the backend's loading function.

    Returns:
        Union[pd.DataFrame, pl.LazyFrame, dd.DataFrame]: A DataFrame containing the loaded data.

    Raises:
        ValueError: If an unsupported backend is specified.
    """
    match backend:
        case "pandas":
            import pandas as pd

            return pd.concat(
                [
                    pd.read_parquet(uri, columns=columns, storage_options=auth_args)
                    for uri in parquet_uris
                ]
            )
        case "polars":
            import polars as pl

            return pl.scan_parquet(
                parquet_uris, storage_options=auth_args, **backend_kwargs
            ).select(columns)
        case "dask":
            import dask.dataframe as dd

            df = dd.read_parquet(
                parquet_uris, storage_options=auth_args, **backend_kwargs
            )
            return df[columns]
        case _:
            raise ValueError(
                f"Unsupported backend: {backend}. Valid options are {Backend.__args__}"
            )


def _parse_filepath_to_partitions(file_path: str) -> dict:
    """Parse a file path to extract partition keys and values.

    Assume the file path is in the format:
        foo/col1=val1/col2=val2/.../colN=valN/bar

    Args:
        file_path (str): The file path to parse.

    Returns:
        dict: A dictionary of partition keys and values.
    """
    partitions = {}
    for part in file_path.split("/"):
        if "=" in part:
            key, value = part.split("=", 1)
            if key not in VALID_PARTITION_KEYS:
                raise InvalidPartitionKeyError(key)
            partitions[key] = value
    return partitions


def load_manifest(parquet_dir: str) -> "pd.DataFrame":
    """Load the manifest file from the parquet directory to a DataFrame.

    We assume Hive-style partitioning on GCS, and create a DataFrame with the
    partition keys and file paths.

    Args:
        parquet_dir (str): Top-level directory containing the Hive-partitioned dataset.

    Returns:
        pd.DataFrame: DataFrame with partition keys, values, and file paths.
    """
    logger.debug("Loading manifest from parquet directory: %s", parquet_dir)
    filesystem = "gcs" if parquet_dir.startswith("gs://") else "file"
    fs = fsspec.filesystem(filesystem, **auth_args)
    files = fs.find(parquet_dir, detail=True)
    logger.debug("Found %d files in the directory.", len(files))
    metadata = []
    for file_path, deets in files.items():
        if not file_path.endswith(".parquet"):
            continue
        # Parse Hive-style partitions
        partition_data = _parse_filepath_to_partitions(file_path)
        prefix = "gs://" if filesystem == "gcs" else ""
        partition_data["file"] = prefix + file_path
        partition_data["size_in_mb"] = deets["size"]
        metadata.append(partition_data)

    if not metadata:
        raise FileNotFoundError(f"No parquet files found in {parquet_dir}")
    df = pd.DataFrame(metadata)
    df["size_in_mb"] = df["size_in_mb"] / (1024 * 1024)

    for time_index in VALID_TIME_INDEX:
        if time_index in df.columns:
            df[time_index] = pd.to_datetime(df[time_index])
            df.set_index(time_index, inplace=True)
            df.index = df.index.tz_localize("UTC")
            break

    return df
