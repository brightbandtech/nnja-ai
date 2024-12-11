from nnja.catalog import generate_catalog


def test_generate_catalog():
    assert generate_catalog() is None
