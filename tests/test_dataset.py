from nnja.dataset import NNJADataset
import numpy as np
import pandas as pd
import json
import pytest
from typing import Dict, Set


@pytest.fixture
def sample_dataset(tmp_path):
    days = 4
    df = pd.DataFrame(
        {
            "time": pd.date_range(start="2021-01-01", periods=days * 4, freq="6h"),
            "lat": np.random.uniform(-90, 90, days * 4),
            "lon": np.random.uniform(-180, 180, days * 4),
            "fovn": np.random.randint(0, 180, days * 4),
            "said": np.random.randint(0, 100, days * 4),
            "brightness_temp_001": np.random.uniform(200, 300, days * 4),
            "brightness_temp_002": np.random.uniform(200, 300, days * 4),
            "brightness_temp_003": np.random.uniform(200, 300, days * 4),
            "brightness_temp_004": np.random.uniform(200, 300, days * 4),
            "brightness_temp_005": np.random.uniform(200, 300, days * 4),
            "std_dev_brightness_temp_001": np.random.uniform(0, 10, days * 4),
            "std_dev_brightness_temp_002": np.random.uniform(0, 10, days * 4),
            "std_dev_brightness_temp_003": np.random.uniform(0, 10, days * 4),
            "std_dev_brightness_temp_004": np.random.uniform(0, 10, days * 4),
            "std_dev_brightness_temp_005": np.random.uniform(0, 10, days * 4),
        }
    )
    df["OBS_DATE"] = df["time"].dt.date
    df.to_parquet(tmp_path / "amsu.parquet", partition_cols=["OBS_DATE"])

    metadata = {
        "name": "AMSU",
        "description": "AMSU data from a satellite",
        "tags": ["satellite", "amsu"],
        "parquet_dir": str(tmp_path),
        "dimensions": [
            {
                "channel": {
                    "description": "AMSU channel number",
                    "values": [1, 2, 3, 4, 5],
                    "format_str": "03.0f",
                }
            }
        ],
        "variables": [
            {
                "id": "lat",
                "description": "Latitude of the observation.",
                "category": "primary descriptors",
                "dimension": None,
            },
            {
                "id": "lon",
                "description": "Longitude of the observation.",
                "category": "primary descriptors",
                "dimension": None,
            },
            {
                "id": "time",
                "description": "Time of the observation.",
                "category": "primary descriptors",
                "dimension": None,
            },
            {
                "id": "said",
                "description": "Satellite ID",
                "category": "secondary descriptors",
                "dimension": None,
            },
            {
                "id": "fovn",
                "description": "Field of view number",
                "category": "secondary descriptors",
                "dimension": None,
            },
            {
                "id": "brightness_temp",
                "description": "Brightness temperature for different channels.",
                "category": "primary data",
                "dimension": "channel",
            },
            {
                "id": "std_dev_brightness_temp",
                "description": "Standard deviation of brightness temperature for different channels.",
                "category": "secondary data",
                "dimension": "channel",
            },
        ],
    }
    with open(tmp_path / "test_dataset.json", "w") as f:
        json.dump(metadata, f)

    dataset = NNJADataset(tmp_path / "test_dataset.json")
    return dataset


def test_dataset_initialization():
    dataset = NNJADataset(
        "tests/sample_data/adpsfc_NC000001_dataset.json", skip_manifest=True
    )
    assert dataset.name == "WMOSYNOP_fixed"
    assert dataset.tags == ["surface", "fixed-station", "synoptic", "wmo"]
    assert dataset.manifest.empty
    assert len(dataset.variables) == 3


def test_variable_expansion():
    dataset = NNJADataset("tests/sample_data/amsu_dataset.json", skip_manifest=True)
    variables = dataset.variables
    channels = [1, 2, 3, 4, 5]
    expected_variables = ["lat", "lon", "time", "said", "fovn"]
    expected_channel_vars = [f"brightness_temp_{channel:05.0f}" for channel in channels]
    expected_variables = expected_variables + expected_channel_vars
    assert set(variables) == set(expected_variables)
    assert dataset.dimensions.keys() == {"channel"}


def test_get_variable():
    dataset = NNJADataset("tests/sample_data/amsu_dataset.json", skip_manifest=True)
    variable = dataset["lat"]
    assert variable.id == "lat"
    assert variable.description == "Latitude of the observation."


class TestDatasetMultivariableSelection:
    """Test class for dataset multivariable selection functionality."""

    def test_subsetting_returns_correct_type(self, sample_dataset):
        """Test that subsetting returns a NNJADataset instance."""
        sub_ds = sample_dataset[["lat", "lon", "time"]]
        assert isinstance(sub_ds, NNJADataset)

    def test_original_dataset_remains_unmodified(self, sample_dataset):
        """Test that the original dataset is not modified when creating a subset."""
        dataset = sample_dataset
        _ = dataset[["lat", "lon", "time"]]
        assert "fovn" in dataset.variables

    def test_subset_contains_only_selected_variables(self, sample_dataset):
        """Test that the subset contains only the specifically selected variables."""
        sub_ds = sample_dataset[["lat", "lon", "time"]]
        expected_variables: Set[str] = {"lat", "lon", "time"}
        assert set(sub_ds.variables) == expected_variables

    def test_subset_matches_select_columns_output(self, sample_dataset):
        """Test that subsetting produces the same result as using _select_columns."""
        dataset = sample_dataset
        selected_vars = ["lat", "lon", "time"]
        sub_ds = dataset[selected_vars]
        sub_ds_2 = dataset._select_columns(selected_vars)
        assert sub_ds.variables.keys() == sub_ds_2.variables.keys()

    def test_unused_dimensions_are_dropped(self, sample_dataset):
        """Test that unused dimensions are dropped in the subset."""
        dataset = sample_dataset
        sub_ds = dataset[["lat", "lon", "time"]]
        expected_dimensions: Dict = {}
        assert sub_ds.dimensions == expected_dimensions

    def test_sel_method_produces_same_result(self, sample_dataset):
        """Test that .sel() method produces the same result as direct subsetting."""
        dataset = sample_dataset
        selected_vars = ["lat", "lon", "time"]
        sub_ds = dataset[selected_vars]
        sub_ds_3 = dataset.sel(variables=selected_vars)
        assert sub_ds.variables.keys() == sub_ds_3.variables.keys()


def test_sel_with_variables_not_in_dataset(sample_dataset):
    dataset = sample_dataset
    with pytest.raises(ValueError):
        _ = dataset.sel(variables=["lat", "lon", "time", "foo"])


def test_manifest_loading(sample_dataset):
    dataset = sample_dataset
    manifest = dataset.manifest
    assert len(manifest) == 4
    expected_bits = [f"OBS_DATE=2021-01-0{i}" for i in range(1, 5)]
    for bit in expected_bits:
        assert any(bit in file for file in manifest["file"])
