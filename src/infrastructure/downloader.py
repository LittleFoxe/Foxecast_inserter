import os
import tempfile
import time
from typing import Tuple

import httpx


def download_to_tempfile(url: str, timeout_seconds: int) -> Tuple[str, int, int]:
    """Download a file to a temporary location and return (path, size_bytes, elapsed_ms) tuple."""
    start = time.perf_counter()
    with httpx.stream("GET", url, timeout=timeout_seconds, follow_redirects=True) as r:
        r.raise_for_status()
        fd, path = tempfile.mkstemp(prefix="forecast_", suffix=os.path.splitext(url)[1])
        size = 0
        with os.fdopen(fd, "wb") as f:
            for chunk in r.iter_bytes():
                f.write(chunk)
                size += len(chunk)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    return path, size, elapsed_ms


