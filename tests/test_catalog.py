from nnja.catalog import DataCatalog, generate_catalog
from nnja.dataset import NNJADataset
import pytest

catalog_path = "tests/sample_data/catalog.json"


@pytest.fixture
def catalog():
    return DataCatalog(
        mirror=None,  # Explicitly disable mirror to use custom parameters
        base_path="tests/sample_data",
        catalog_json="catalog.json",
        skip_manifest=True,
    )


def test_generate_catalog():
    assert generate_catalog() is None


def test_catalog_initialization(catalog):
    assert catalog.catalog_uri == "tests/sample_data/catalog.json"
    assert catalog.base_path == "tests/sample_data"
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


def test_mirror_path_resolution():
    """Test that mirror configuration resolves paths correctly."""
    from nnja.catalog import MIRRORS, _resolve_path

    # Test that mirrors are configured correctly
    assert "gcp_nodd" in MIRRORS
    assert "base_path" in MIRRORS["gcp_nodd"]
    assert "catalog_json" in MIRRORS["gcp_nodd"]

    # Test relative path resolution
    base_path = "gs://test-bucket/data"
    catalog_json = "catalog.json"
    expected_uri = "gs://test-bucket/data/catalog.json"
    resolved_uri = _resolve_path(base_path, catalog_json)
    assert resolved_uri == expected_uri

    # Test absolute cloud URI passthrough
    absolute_cloud_uri = "gs://other-bucket/catalog.json"
    resolved_uri = _resolve_path(base_path, absolute_cloud_uri)
    assert resolved_uri == absolute_cloud_uri

    # Test absolute local path passthrough
    absolute_local_path = "/tmp/local/catalog.json"
    resolved_uri = _resolve_path(base_path, absolute_local_path)
    assert resolved_uri == absolute_local_path


def test_custom_initialization():
    """Test that DataCatalog can be initialized with custom base_path and catalog_json."""
    catalog = DataCatalog(
        mirror=None,  # Explicitly disable mirror to use custom parameters
        base_path="tests/sample_data",
        catalog_json="catalog.json",
        skip_manifest=True,
    )
    assert catalog.base_path == "tests/sample_data"
    assert catalog.catalog_uri == "tests/sample_data/catalog.json"


def test_mirror_and_custom_parameters_error():
    """Test that specifying both mirror and custom parameters raises an error."""
    with pytest.raises(
        ValueError, match="Cannot specify both 'mirror' and custom parameters"
    ):
        DataCatalog(mirror="gcp_nodd", base_path="custom/path")

    with pytest.raises(
        ValueError, match="Cannot specify both 'mirror' and custom parameters"
    ):
        DataCatalog(mirror="gcp_nodd", catalog_json="custom.json")


def test_custom_parameters_incomplete_error():
    """Test that specifying only one custom parameter raises an error."""
    with pytest.raises(
        ValueError, match="both 'base_path' and 'catalog_json' must be specified"
    ):
        DataCatalog(mirror=None, base_path="custom/path")

    with pytest.raises(
        ValueError, match="both 'base_path' and 'catalog_json' must be specified"
    ):
        DataCatalog(mirror=None, catalog_json="custom.json")
