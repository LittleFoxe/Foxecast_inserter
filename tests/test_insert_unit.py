import shutil
from pathlib import Path
from fastapi.testclient import TestClient

from src.main import app
from src.infrastructure.service_provider import get_downloader


client = TestClient(app)


def test_health_ok():
    r = client.get("/health")
    assert r.status_code == 200


def test_insert_local_file(monkeypatch, tmp_path):
    # We bypass network by monkeypatching downloader to simply return local path
    source_file = Path(__file__).parent / "sample.grib2"
    local_file = tmp_path / "sample.grib2"
    shutil.copy2(source_file, local_file)

    def fake_download(url: str, timeout: int):
        return str(local_file), len(local_file.read_bytes()), 1

    # Override downloader dependency to ensure controller uses fake implementation
    app.dependency_overrides[get_downloader] = lambda: fake_download

    r = client.post("/insert", json={"url": "http://localhost/sample.grib2"})
    assert r.status_code == 200, f"Expected status 200, received {r.status_code}. Response body: {r.text}"

    # Cleanup overrides for isolation
    app.dependency_overrides.clear()
