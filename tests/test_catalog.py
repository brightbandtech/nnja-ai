from nnja.catalog import DataCatalog, generate_catalog
from nnja.dataset import NNJADataset
import pytest

catalog_path = "tests/sample_data/catalog.json"


@pytest.fixture
def catalog():
    return DataCatalog(catalog_path, skip_manifest=True)


def test_generate_catalog():
    assert generate_catalog() is None


def test_catalog_initialization(catalog):
    assert catalog.json_uri == catalog_path
    assert isinstance(catalog.catalog_metadata, dict)
    assert isinstance(catalog.datasets, dict)
    assert all(
        isinstance(dataset, NNJADataset) for dataset in catalog.datasets.values()
    )


def test_catalog_getitem(catalog):
    dataset = catalog["amsu"]
    assert dataset.name == "amsu_sample_data"
    assert (
        dataset.description
        == "Sample AMSU dataset with temperature brightness values and geospatial metadata."
    )


def test_catalog_info(catalog):
    info = catalog.info()
    assert "amsu" in info
    assert "adpsfc" in info


def test_catalog_list_datasets(catalog):
    datasets = catalog.list_datasets()
    assert "amsu" in datasets
    assert "adpsfc_WMOSYNOP_fixed" in datasets


def test_catalog_search(catalog):
    results = catalog.search("Brightness Temperature")
    result_names = [dataset.name for dataset in results]
    assert result_names == ["amsu_sample_data"]


def _test_catalog_search_by_tag(catalog):
    results = catalog.search("synoptic")
    result_names = [dataset.name for dataset in results]
    assert set(result_names) == {"adpsfc_WMOSYNOP_fixed", "adpsfc_WMOSYNOP_mobile"}
