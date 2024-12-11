import fsspec
import json
from typing import Literal, List, Union, TYPE_CHECKING

Backend = Literal["pandas", "polars", "dask"]

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import dask.dataframe as dd


def read_json(json_uri: str) -> dict:
    """Read a JSON file from a URI. Supports local and cloud storage."""
    with fsspec.open(json_uri, mode="r") as f:
        return json.load(f)


def load_parquet(
    parquet_uris: List[str],
    columns: List[str],
    backend: Backend = "pandas",
    **backend_kwargs,
) -> Union["pd.DataFrame", "pl.LazyFrame", "dd.DataFrame"]:  # noqa: F821 # type: ignore
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
