import fsspec
import json
from typing import Literal, List

Backend = Literal["pandas", "polars", "dask"]


def read_json(json_uri: str) -> dict:
    with fsspec.open(json_uri, mode="r") as f:
        return json.load(f)


def load_parquet(
    parquet_uris: List[str],
    columns: List[str],
    backend: Backend = "pandas",
    **backend_kwargs,
):
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
