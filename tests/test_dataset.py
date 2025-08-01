from nnja.dataset import NNJADataset
import numpy as np
import pandas as pd
import json
import pytest
from typing import Dict, Set
import warnings

from nnja.exceptions import EmptyTimeSubsetError


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
        "parquet_root_path": str(tmp_path),
        "dimensions": [
            {
                "channel": {
                    "description": "AMSU channel number",
                    "values": [1, 2, 3, 4, 5],
                    "format_str": "03.0f",
                    "units": None,
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

    dataset = NNJADataset(str(tmp_path / "test_dataset.json"), base_path=str(tmp_path))
    return dataset


def test_dataset_initialization():
    dataset = NNJADataset(
        "tests/sample_data/adpsfc_NC000001_dataset.json",
        base_path="tests/sample_data",
        skip_manifest=True,
    )
    assert dataset.name == "WMOSYNOP_fixed"
    assert dataset.tags == ["surface", "fixed-station", "synoptic", "wmo"]
    assert dataset.manifest.empty
    assert len(dataset.variables) == 3


def test_variable_expansion():
    dataset = NNJADataset(
        "tests/sample_data/amsu_dataset.json",
        base_path="tests/sample_data",
        skip_manifest=True,
    )
    variables = dataset.variables
    channels = [1, 2, 3, 4, 5]
    expected_variables = ["lat", "lon", "time", "said", "fovn"]
    expected_channel_vars = [f"brightness_temp_{channel:05.0f}" for channel in channels]
    expected_variables = expected_variables + expected_channel_vars
    assert set(variables) == set(expected_variables)
    assert dataset.dimensions.keys() == {"channel"}


def test_get_variable():
    dataset = NNJADataset(
        "tests/sample_data/amsu_dataset.json",
        base_path="tests/sample_data",
        skip_manifest=True,
    )
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


class TestTimeSelectionTimeZoneBehavior:
    """Test class for time selection and time zone behavior."""

    def test_select_time_single_timestamp_notz(self, sample_dataset):
        # Test that selecting a single nontz timestamp works as expected, returns same but with UTC tz
        dataset = sample_dataset
        timestamp = pd.Timestamp("2021-01-01")
        with warnings.catch_warnings(record=True):
            with pytest.warns(
                UserWarning,
                match="Naive datetime 2021-01-01 00:00:00 assumed to be in UTC",
            ):
                subset = dataset.sel(time=timestamp)
        assert len(subset.manifest) == 1
        assert subset.manifest.index[0] == timestamp.tz_localize("UTC")

    def test_select_time_slice_notz(self, sample_dataset):
        # Test that selecting a nontz time slice works as expected, returns same but with UTC tz
        dataset = sample_dataset
        time_slice = slice(pd.Timestamp("2021-01-01"), pd.Timestamp("2021-01-02"))
        with warnings.catch_warnings(record=True):
            with pytest.warns(
                UserWarning,
                match="Naive datetime 2021-01-01 00:00:00 assumed to be in UTC",
            ):
                subset = dataset.sel(time=time_slice)
        assert len(subset.manifest) == 2  # since it's partitioned by day
        assert subset.manifest.index.min() == pd.Timestamp(
            "2021-01-01 00:00:00", tz="UTC"
        )
        assert subset.manifest.index.max() == pd.Timestamp(
            "2021-01-02 00:00:00", tz="UTC"
        )

    def test_select_time_list_notz(self, sample_dataset):
        # Test that selecting a list of nontz timestamps works as expected, returns same but with UTC tz
        dataset = sample_dataset
        timestamps = [
            pd.Timestamp("2021-01-01 00:00:00"),
            pd.Timestamp("2021-01-03 00:00:00"),
        ]
        with warnings.catch_warnings(record=True):
            with pytest.warns(
                UserWarning,
                match="Naive datetime 2021-01-01 00:00:00 assumed to be in UTC",
            ):
                subset = dataset.sel(time=timestamps)
        assert len(subset.manifest) == 2
        assert all(ts.tz_localize("UTC") in subset.manifest.index for ts in timestamps)

    def test_select_with_str_timestamp_notz(self, sample_dataset):
        # Test that selecting a string timestamp works as expected, returns same but with UTC tz
        dataset = sample_dataset
        timestamp = "2021-01-01"
        with warnings.catch_warnings(record=True):
            with pytest.warns(
                UserWarning,
                match="Naive datetime 2021-01-01 00:00:00 assumed to be in UTC",
            ):
                subset = dataset.sel(time=timestamp)
        assert len(subset.manifest) == 1
        assert subset.manifest.index[0] == pd.Timestamp(timestamp).tz_localize("UTC")

    def test_select_time_single_timestamp_utc(self, sample_dataset):
        dataset = sample_dataset
        timestamp = pd.Timestamp("2021-01-01", tz="UTC")
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("error")
            subset = dataset.sel(time=timestamp)
        assert len(subset.manifest) == 1
        assert subset.manifest.index[0] == timestamp

    def test_select_time_slice_utc(self, sample_dataset):
        dataset = sample_dataset
        time_slice = slice(
            pd.Timestamp("2021-01-01", tz="UTC"), pd.Timestamp("2021-01-02", tz="UTC")
        )
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("error")
            subset = dataset.sel(time=time_slice)
        assert len(subset.manifest) == 2  # since it's partitioned by day
        assert subset.manifest.index.min() == pd.Timestamp("2021-01-01", tz="UTC")
        assert subset.manifest.index.max() == pd.Timestamp("2021-01-02", tz="UTC")

    def test_select_time_list_utc(self, sample_dataset):
        dataset = sample_dataset
        timestamps = [
            pd.Timestamp("2021-01-01", tz="UTC"),
            pd.Timestamp("2021-01-03", tz="UTC"),
        ]
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            subset = dataset.sel(time=timestamps)
        assert len(subset.manifest) == 2
        assert all(ts in subset.manifest.index for ts in timestamps)

    def test_select_with_different_timezone(self, sample_dataset):
        # Note: this is stupid behavior, but it is conceivable that someone might do this
        dataset = sample_dataset
        timestamp = pd.Timestamp("2020-12-31 16:00:00", tz="US/Pacific")
        with warnings.catch_warnings(record=True):
            with pytest.warns(
                UserWarning, match="Non-UTC timezone US/Pacific converted to UTC"
            ):
                subset = dataset.sel(time=timestamp)
            subset = dataset.sel(time=timestamp)
        assert len(subset.manifest) == 1
        assert subset.manifest.index[0] == pd.Timestamp("2021-01-01", tz="UTC")

    def test_empty_time_subset(self, sample_dataset):
        dataset = sample_dataset
        with warnings.catch_warnings(record=True):
            with pytest.raises(
                EmptyTimeSubsetError, match="Time subset resulted in an empty DataFrame"
            ):
                dataset.sel(
                    time=slice(pd.Timestamp("1900-01-01"), pd.Timestamp("1900-01-02"))
                )


def test_select_both_time_and_variables(sample_dataset):
    dataset = sample_dataset
    timestamp = pd.Timestamp("2021-01-03 00:00:00", tz="UTC")
    variables = ["lat", "lon", "time"]
    subset = dataset.sel(time=timestamp, variables=variables)
    assert len(subset.manifest) == 1
    assert subset.manifest.index[0] == timestamp
    assert set(subset.variables) == set(variables)


def test_select_and_load_with_time_and_variables(sample_dataset):
    dataset = sample_dataset
    timestamp = pd.Timestamp("2021-01-03")
    variables = ["lat", "lon", "time"]
    with warnings.catch_warnings(record=True):
        with pytest.warns(
            UserWarning, match="Naive datetime 2021-01-03 00:00:00 assumed to be in UTC"
        ):
            subset = dataset.sel(time=timestamp, variables=variables)
    df = subset.load_dataset()
    assert len(df) == 4
    assert set(df.columns) == set(variables)


def test_bad_time_range(sample_dataset):
    dataset = sample_dataset
    time_slice = slice(
        pd.Timestamp("2022-01-01 00:00:00"), pd.Timestamp("2022-01-02 00:18:00")
    )
    with warnings.catch_warnings(record=True):
        with pytest.raises(EmptyTimeSubsetError):
            _ = dataset.sel(time=time_slice)


@pytest.mark.parametrize(
    "time_sel",
    [
        pd.Timestamp("2022-01-01 12:00:00"),
        [pd.Timestamp("2022-01-01 00:00:00"), pd.Timestamp("2022-01-02 00:00:00")],
    ],
)
def test_time_selection_with_invalid_time(sample_dataset, time_sel):
    dataset = sample_dataset
    with pytest.raises(KeyError):
        with warnings.catch_warnings(record=True):
            _ = dataset.sel(time=time_sel)


@pytest.mark.parametrize(
    "dim_name, selection, expected_values",
    [
        ("channel", 1, [1]),
        ("channel", [1, 3, 5], [1, 3, 5]),
        ("channel", slice(1, 3), [1, 2, 3]),
    ],
)
def test_select_extra_dimension(sample_dataset, dim_name, selection, expected_values):
    dataset = sample_dataset
    subset = dataset._select_extra_dimension(dim_name, selection)
    assert set(subset.dimensions[dim_name]["values"]) == set(expected_values)
    for var in subset.variables.values():
        if var.dimension == dim_name:
            assert var.dim_val in expected_values


def test_select_extra_dimension_invalid_value(sample_dataset):
    dataset = sample_dataset
    with pytest.raises(ValueError, match="Value '10' not found in dimension 'channel'"):
        _ = dataset._select_extra_dimension("channel", 10)


def test_select_extra_dimension_invalid_list(sample_dataset):
    dataset = sample_dataset
    with pytest.raises(
        ValueError, match=r"Values \[10, 20\] not found in dimension 'channel'"
    ):
        _ = dataset._select_extra_dimension("channel", [10, 20])


def test_select_extra_dimension_invalid_slice(sample_dataset):
    dataset = sample_dataset
    with pytest.raises(ValueError, match="Slice must have at least one bound"):
        _ = dataset._select_extra_dimension("channel", slice(None, None))


@pytest.mark.parametrize(
    "selection",
    [
        slice(0, 3),
        slice(1, 6),
        slice(1.5, 5),
    ],
)
def test_select_extra_dimension_bad_slice_values(sample_dataset, selection):
    dataset = sample_dataset
    with pytest.raises(ValueError, match="is not in list"):
        _ = dataset._select_extra_dimension("channel", selection)


def test_select_extra_dimension_step_not_supported(sample_dataset):
    dataset = sample_dataset
    with pytest.raises(
        NotImplementedError, match="Step not supported for slicing dimensions"
    ):
        _ = dataset._select_extra_dimension("channel", slice(1, 5, 2))
