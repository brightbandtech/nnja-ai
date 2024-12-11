from nnja.dataset import NNJADataset
import numpy as np
import pandas as pd
import json
import pytest


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
    df["date"] = df["time"].dt.date
    df.to_parquet(tmp_path / "amsu.parquet", partition_cols=["date"])
    files = [f"amsu.parquet/date={date}" for date in df["date"].unique()]

    metadata = {
        "name": "AMSU",
        "description": "AMSU data from a satellite",
        "tags": ["satellite", "amsu"],
        "manifest": files,
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
    dataset = NNJADataset("tests/sample_data/adpsfc_NC000001_dataset.json")
    assert dataset.name == "WMOSYNOP_fixed"
    assert dataset.tags == ["surface", "fixed-station", "synoptic", "wmo"]
    assert len(dataset.manifest) == 0
    assert len(dataset.variables) == 3


def test_variable_expansion():
    dataset = NNJADataset("tests/sample_data/amsu_dataset.json")
    variables = dataset.variables
    channels = [1, 2, 3, 4, 5]
    expected_variables = ["lat", "lon", "time", "said", "fovn"]
    expected_channel_vars = [f"brightness_temp_{channel:05.0f}" for channel in channels]
    expected_variables = expected_variables + expected_channel_vars
    assert set(variables) == set(expected_variables)
    assert dataset.dimensions.keys() == {"channel"}


def test_get_variable():
    dataset = NNJADataset("tests/sample_data/amsu_dataset.json")
    variable = dataset["lat"]
    assert variable.id == "lat"
    assert variable.description == "Latitude of the observation."


def test_dataset_multivariable_selection_with_dim_drop(sample_dataset):
    dataset = sample_dataset
    sub_ds = dataset[["lat", "lon", "time"]]
    assert isinstance(sub_ds, NNJADataset)

    # check that the original dataset is not modified
    assert "fovn" in dataset.variables

    # check that the new dataset has only the selected variables
    assert set(sub_ds.variables) == {"lat", "lon", "time"}

    # assert that it's the same as select_columns
    sub_ds_2 = dataset._select_columns(["lat", "lon", "time"])
    assert sub_ds.variables.keys() == sub_ds_2.variables.keys()

    # assert that the unused dims are dropped
    assert sub_ds.dimensions == {}

    # assert that using .sel() does the same thing
    sub_ds_3 = dataset.sel(variables=["lat", "lon", "time"])
    assert sub_ds.variables.keys() == sub_ds_3.variables.keys()


def test_sel_with_variables_not_in_dataset(sample_dataset):
    dataset = sample_dataset
    with pytest.raises(ValueError):
        _ = dataset.sel(variables=["lat", "lon", "time", "foo"])
