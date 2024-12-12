import fsspec
import json
from typing import Literal, List, Union, TYPE_CHECKING
import pandas as pd
import logging

from nnja.exceptions import InvalidPartitionKeyError

if TYPE_CHECKING:
    import polars as pl
    import dask.dataframe as dd

VALID_TIME_INDEX = ["OBS_DATE", "OBS_HOUR"]
VALID_PARTITION_KEYS = ["OBS_DATE", "OBS_HOUR", "MSG_TYPE"]

logger = logging.getLogger(__name__)

Backend = Literal["pandas", "polars", "dask"]


def read_json(json_uri: str) -> dict:
    """Read a JSON file from a URI. Supports local and cloud storage."""
    with fsspec.open(json_uri, mode="r") as f:
        return json.load(f)


def load_parquet(
    parquet_uris: List[str],
    columns: List[str],
    backend: Backend = "pandas",
    **backend_kwargs,
) -> Union["pd.DataFrame", "pl.LazyFrame", "dd.DataFrame"]:
    """Load parquet files using the specified backend; lazy if supported by the backend.
    With the current implementation, polars and dask will load lazily and preserve any
    hive partitions + columns, while pandas will load eagerly and concatenate the dataframes.

    Parameters:
    parquet_uris (List[str]): List of URIs pointing to the parquet files.
    columns (List[str]): List of columns to load from the parquet files.
    backend (str, optional): Backend to use for loading the parquet files.
                             Valid options are "pandas", "polars", and "dask".
                             Default is "pandas".
    **backend_kwargs: Additional keyword arguments to pass to the backend's
                      loading function.
    Returns:
    Union[pd.DataFrame, pl.LazyFrame, dd.DataFrame]: A DataFrame containing the loaded data.

    Raises:
    ValueError: If an unsupported backend is specified.
    """
    match backend:
        case "pandas":
            import pandas as pd

            return pd.concat(
                [pd.read_parquet(uri, columns=columns) for uri in parquet_uris]
            )
            # TODO I think this might be better if we used filters to do the time subsetting
        case "polars":
            import polars as pl

            return pl.scan_parquet(parquet_uris, **backend_kwargs).select(columns)
        case "dask":
            import dask.dataframe as dd

            return dd.read_parquet(parquet_uris, columns=columns, **backend_kwargs)
        case _:
            raise ValueError(
                f"Unsupported backend: {backend}. valid options are {Backend.__args__}"
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
    fs = fsspec.filesystem(filesystem)
    files = fs.find(parquet_dir, detail=True)
    logger.debug("Found %d files in the directory.", len(files))
    metadata = []
    for file_path, deets in files.items():
        if not file_path.endswith(".parquet"):
            continue
        # Parse Hive-style partitions
        partition_data = _parse_filepath_to_partitions(file_path)
        partition_data["file"] = file_path
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
            break

    return df
