from nnja.variable import NNJAVariable


def test_variable_repr_and_info_code_table():
    variable_metadata = {
        "id": "CORN",
        "description": "Corrected report indicator",
        "category": "secondary descriptors",
        "extra_metadata": {
            "units": "Code table",
            "code table": "033215",
            "code table link": "https://www.nco.ncep.noaa.gov/sib/jeff/CodeFlag_0_STDv31_LOC7.html#033215",
        },
    }
    var = NNJAVariable(variable_metadata, full_id="CORN")
    assert var.is_code_or_flag_table
    assert "[code table: 033215]" in repr(var)
    info = var.info()
    assert "Code Table: 033215" in info
    assert (
        "Code Table Link: https://www.nco.ncep.noaa.gov/sib/jeff/CodeFlag_0_STDv31_LOC7.html#033215"
        in info
    )


def test_variable_repr_and_info_flag_table():
    variable_metadata = {
        "id": "WNDSQ1.TIWM",
        "description": "Type of instrumentation for wind measurement",
        "category": "secondary descriptors",
        "extra_metadata": {
            "units": "Flag table",
            "flag table": "002002",
            "flag table link": "https://www.nco.ncep.noaa.gov/sib/jeff/CodeFlag_0_STDv31_LOC7.html#002002",
        },
    }
    var = NNJAVariable(variable_metadata, full_id="WNDSQ1.TIWM")
    assert var.is_code_or_flag_table
    assert "[flag table: 002002]" in repr(var)
    info = var.info()
    assert "Flag Table: 002002" in info
    assert (
        "Flag Table Link: https://www.nco.ncep.noaa.gov/sib/jeff/CodeFlag_0_STDv31_LOC7.html#002002"
        in info
    )


def test_variable_repr_and_info_no_table():
    variable_metadata = {
        "id": "lat",
        "description": "Latitude of the observation.",
        "category": "primary descriptors",
        "extra_metadata": {},
    }
    var = NNJAVariable(variable_metadata, full_id="lat")
    assert not var.is_code_or_flag_table
    assert "[code table:" not in repr(var)
    assert "[flag table:" not in repr(var)
    info = var.info()
    assert "Code Table:" not in info
    assert "Flag Table:" not in info
