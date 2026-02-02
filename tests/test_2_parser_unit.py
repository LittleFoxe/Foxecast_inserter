from pathlib import Path

import pytest

from src.services.parser_service import ParserService


def test_parser_returns_complete_dto():
    # Using sample file for testing the functionality of the parser unit
    test_file = Path(__file__).parent / "sample.grib2"

    parser = ParserService()

    try:
        dtos, _ = parser.parse_file(str(test_file), file_name="sample.grib2")
    except Exception:
        # If earthkit cannot parse the dummy file, the unit test cannot proceed.
        pytest.fail("Cannot parse local testing file!")

    assert dtos, "Parser should return at least one DTO"
    d = dtos[0]
    # Verifying all DTO fields are non-empty/non-None and sensible
    # TODO: Make verification based on real values from GRIB-file
    assert d.id
    assert d.forecast_date is not None
    assert isinstance(d.forecast_hour, int)
    assert d.data_source
    assert d.parameter
    assert d.parameter_unit is not None
    assert d.surface_type
    assert isinstance(d.surface_value, float)
    assert isinstance(d.min_lon, float)
    assert isinstance(d.max_lon, float)
    assert isinstance(d.min_lat, float)
    assert isinstance(d.max_lat, float)
    assert isinstance(d.lon_step, float)
    assert isinstance(d.lat_step, float)
    assert isinstance(d.grid_size_lat, int)
    assert isinstance(d.grid_size_lon, int)
    assert isinstance(d.values, list) and len(d.values) > 0
    assert d.file_name == "sample.grib2"
