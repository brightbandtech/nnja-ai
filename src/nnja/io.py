import fsspec
import json
from typing import Literal

Backend = Literal["pandas", "polars", "dask"]


def read_json(json_uri: str) -> dict:
    with fsspec.open(json_uri, mode="r") as f:
        return json.load(f)
