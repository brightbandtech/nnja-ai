from nnja.dataset import NNJADataset


def test_dataset_initialization():
    dataset = NNJADataset("tests/sample_data/adpsfc_nc000001_dataset.json")
    assert dataset.name == "WMOSYNOP_fixed"
    assert (
        dataset.description
        == "Synoptic - fixed land (originating from WMO SYNOP bulletins)."
    )
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
