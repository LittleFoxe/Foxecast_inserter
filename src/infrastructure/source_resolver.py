import re


def resolve_data_source(file_name: str, fallback: str = "unknown") -> str:
    """Resolve data_source from file_name using simple heuristics.

    This allows overriding GRIB metadata for cases like graphcast/panguweather.
    """
    name = file_name.lower()
    # Several patterns for getting the data source from filename
    patterns = {
        r"ecmwf|era5|ifs": "ecmwf",
        r"gfs|noaa": "gfs",
        r"icon|dwd": "icon",
    }
    for pat, src in patterns.items():
        if re.search(pat, name):
            return src
    return fallback
