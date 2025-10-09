import os
import time
from typing import List, Tuple

from src.domain.dto import ForecastDataDTO
from src.services.parsers import GribParser, BufrParser


class ParserService:
    """Parses hydrometeorological data files (GRIB/BUFR) into DTOs using strategies."""

    def _detect_format(self, path: str) -> str:
        ext = os.path.splitext(path)[1].lower()
        if ext in (".grib", ".grb", ".grib2", ".grb2"):
            return "grib"
        if ext in (".bufr", ):
            return "bufr"
        # Fallback by magic header
        with open(path, "rb") as f:
            magic = f.read(4)
            if magic == b"GRIB":
                return "grib"
            if magic == b"BUFR":
                return "bufr"
        return "unknown"

    def parse_file(self, local_path: str, file_name: str) -> Tuple[List[ForecastDataDTO], int]:
        start = time.perf_counter()

        fmt = self._detect_format(local_path)
        if fmt == "grib":
            parser = GribParser()
            dtos = parser.parse(local_path, file_name)
        elif fmt == "bufr":
            parser = BufrParser()
            dtos = parser.parse(local_path, file_name)
        else:
            raise ValueError("Unsupported file format. Expected GRIB or BUFR")

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return dtos, elapsed_ms
