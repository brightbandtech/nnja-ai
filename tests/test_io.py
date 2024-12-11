import pytest
from nnja.io import load_parquet, read_json
import pandas as pd
import polars as pl
import json
import fsspec

import dask.dataframe as dd


@pytest.fixture
def sample_parquet_files(tmp_path):
    # Create sample parquet files for testing
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df2 = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    file1 = tmp_path / "file1.parquet"
    file2 = tmp_path / "file2.parquet"
    df.to_parquet(file1)
    df2.to_parquet(file2)
    return [str(file1), str(file2)]


@pytest.fixture
def sample_json_file(tmp_path):
    # Create a sample JSON file for testing
    data = {"key1": "value1", "key2": "value2"}
    file_path = tmp_path / "sample.json"
    with fsspec.open(file_path, mode="w") as f:
        json.dump(data, f)
    return str(file_path)


def test_load_parquet_pandas(sample_parquet_files):
    result = load_parquet(sample_parquet_files, columns=["a"], backend="pandas")
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["a"]
    assert result["a"].tolist() == [1, 2, 3, 7, 8, 9]


def test_load_parquet_polars(sample_parquet_files):
    result = load_parquet(sample_parquet_files, columns=["a"], backend="polars")
    assert isinstance(result, pl.LazyFrame)
    result = result.collect()
    assert result.columns == ["a"]
    assert result["a"].to_list() == [1, 2, 3, 7, 8, 9]


def test_load_parquet_dask(sample_parquet_files):
    result = load_parquet(sample_parquet_files, columns=["a"], backend="dask")
    assert isinstance(result, dd.DataFrame)
    assert list(result.columns) == ["a"]
    assert result["a"].compute().tolist() == [1, 2, 3, 7, 8, 9]


def test_load_parquet_invalid_backend(sample_parquet_files):
    with pytest.raises(
        ValueError, match="Unsupported backend: invalid_backend. valid options are"
    ):
        load_parquet(sample_parquet_files, columns=["a"], backend="invalid_backend")


def test_read_json(sample_json_file):
    result = read_json(sample_json_file)
    assert isinstance(result, dict)
    assert result == {"key1": "value1", "key2": "value2"}
