import json

import dask.dataframe as dd
import fsspec
import google.auth
import pandas as pd
import polars as pl
import pytest

from nnja_ai.exceptions import InvalidPartitionKeyError
from nnja_ai.io import _get_auth_args, _parse_filepath_to_partitions, load_manifest, load_parquet, read_json


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
        ValueError, match="Unsupported backend: invalid_backend. Valid options are"
    ):
        load_parquet(sample_parquet_files, columns=["a"], backend="invalid_backend")


def test_read_json(sample_json_file):
    result = read_json(sample_json_file)
    assert isinstance(result, dict)
    assert result == {"key1": "value1", "key2": "value2"}


def test_parse_filepath_to_partitions_valid():
    file_path = "foo/OBS_DATE=2023-01-01/OBS_HOUR=12/bar.parquet"
    expected = {"OBS_DATE": "2023-01-01", "OBS_HOUR": "12"}
    result = _parse_filepath_to_partitions(file_path)
    assert result == expected


def test_parse_filepath_to_partitions_invalid_key():
    file_path = "foo/INVALID_KEY=2023-01-01/OBS_HOUR=12/bar.parquet"
    with pytest.raises(InvalidPartitionKeyError):
        _parse_filepath_to_partitions(file_path)


def test_parse_filepath_to_partitions_no_partitions():
    file_path = "foo/bar.parquet"
    result = _parse_filepath_to_partitions(file_path)
    assert result == {}


class TestLoadManifest:
    """Test class for load_manifest"""

    @pytest.fixture(autouse=True)
    def setup_method(self, tmp_path, monkeypatch):
        """Setup method to create sample parquet files and directories for testing"""
        self.parquet_dir = tmp_path / "parquet_dir"
        self.parquet_dir.mkdir()
        self.file1 = self.parquet_dir / "OBS_DATE=2023-01-01/file1.parquet"
        self.file2 = self.parquet_dir / "OBS_DATE=2023-01-01/file2.parquet"
        self.file1.parent.mkdir(parents=True, exist_ok=True)
        self.file2.parent.mkdir(parents=True, exist_ok=True)
        self.file1.touch()
        self.file2.touch()

        # Mock fsspec.filesystem to return the sample files
        def mock_find(parquet_dir, detail):
            return {
                str(self.file1): {"size": 1024 * 1024},
                str(self.file2): {"size": 2048 * 1024},
            }

        monkeypatch.setattr(
            "fsspec.filesystem",
            lambda _: type("MockFS", (object,), {"find": mock_find}),
        )

        # Load the manifest once for all tests
        self.result = load_manifest(str(self.parquet_dir))

    def test_load_manifest_returns_dataframe(self):
        """Test that load_manifest returns a DataFrame"""
        assert isinstance(self.result, pd.DataFrame)

    def test_columns_present(self):
        """Test that the expected columns are present in the DataFrame"""
        assert "file" in self.result.columns
        assert "size_in_mb" in self.result.columns

    def test_index_set_correctly(self):
        """Test that the index is set correctly to OBS_DATE"""
        assert self.result.index.name == "OBS_DATE"
        assert self.result.index.tolist() == [
            pd.Timestamp("2023-01-01 00:00:00 UTC"),
            pd.Timestamp("2023-01-01 00:00:00 UTC"),
        ]

    def test_correct_files_and_sizes(self):
        """Test that the correct files are listed in the DataFrame"""
        assert self.result["file"].tolist() == [str(self.file1), str(self.file2)]
        assert self.result["size_in_mb"].tolist() == [1.0, 2.0]


def test_load_manifest_no_parquet_files(tmp_path, monkeypatch):
    # Create an empty directory for testing
    parquet_dir = tmp_path / "parquet_dir"
    parquet_dir.mkdir()

    # Mock fsspec.filesystem to return no files
    def mock_find(parquet_dir, detail):
        return {}

    monkeypatch.setattr(
        "fsspec.filesystem", lambda _: type("MockFS", (object,), {"find": mock_find})
    )

    with pytest.raises(FileNotFoundError, match="No parquet files found in"):
        _ = load_manifest(str(parquet_dir))


def test_load_manifest_invalid_partition_key(tmp_path, monkeypatch):
    # Create sample parquet files with an invalid partition key
    parquet_dir = tmp_path / "parquet_dir"
    parquet_dir.mkdir()
    file1 = parquet_dir / "INVALID_KEY=2023-01-01/OBS_HOUR=12/file1.parquet"
    file1.parent.mkdir(parents=True, exist_ok=True)
    file1.touch()

    # Mock fsspec.filesystem to return the sample files
    def mock_find(parquet_dir, detail):
        return {
            str(file1): {"size": 1024 * 1024},
        }

    monkeypatch.setattr(
        "fsspec.filesystem", lambda _: type("MockFS", (object,), {"find": mock_find})
    )

    with pytest.raises(InvalidPartitionKeyError):
        load_manifest(str(parquet_dir))


def test_get_auth_args_non_gcs():
    """Test that non-GCS URIs return empty auth args"""
    assert _get_auth_args("file:///path/to/file") == {}
    assert _get_auth_args("http://example.com") == {}
    assert _get_auth_args("/local/path") == {}


def test_get_auth_args_gcs_anon_default(monkeypatch):
    """Test that GCS URIs use anonymous credentials by default"""
    # Ensure environment variable is not set
    monkeypatch.delenv("NNJA_USE_AUTH", raising=False)

    result = _get_auth_args("gs://bucket/path")
    assert result == {"token": "anon"}


def test_get_auth_args_gcs_auth(monkeypatch):
    """Test that GCS URIs use authenticated credentials when NNJA_USE_AUTH=true"""
    # Set environment variable to require auth
    monkeypatch.setenv("NNJA_USE_AUTH", "true")

    # Mock successful authentication
    class MockCredentials:
        def __init__(self):
            self.valid = True

    mock_credentials = MockCredentials()
    monkeypatch.setattr(google.auth, "default", lambda: (mock_credentials, "project"))

    result = _get_auth_args("gs://bucket/path")
    assert result == {}


def test_get_auth_args_gcs_auth_failure(monkeypatch):
    """Test that GCS URIs fall back to anonymous credentials when auth fails"""
    # Set environment variable to require auth
    monkeypatch.setenv("NNJA_USE_AUTH", "true")

    # Mock authentication failure
    def mock_default():
        raise google.auth.exceptions.DefaultCredentialsError()

    monkeypatch.setattr(google.auth, "default", mock_default)

    result = _get_auth_args("gs://bucket/path")
    assert result == {"token": "anon"}


def test_get_auth_args_gcs_auth_refresh_failure(monkeypatch):
    """Test that GCS URIs fall back to anonymous credentials when token refresh fails"""
    # Set environment variable to require auth
    monkeypatch.setenv("NNJA_USE_AUTH", "true")

    # Mock credentials that fail to refresh
    class MockCredentials:
        def __init__(self):
            self.valid = False

        def refresh(self, request):
            raise google.auth.exceptions.RefreshError()

    mock_credentials = MockCredentials()
    monkeypatch.setattr(google.auth, "default", lambda: (mock_credentials, "project"))

    result = _get_auth_args("gs://bucket/path")
    assert result == {"token": "anon"}
    assert result == {"token": "anon"}
