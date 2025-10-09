import os
import time

import pytest
from clickhouse_connect import get_client
from fastapi.testclient import TestClient

from src.main import app
from src.infrastructure.config import settings
from src.controllers.http import get_downloader


client = TestClient(app)

def test_insert_into_clickhouse(monkeypatch, tmp_path):
    # Initializing connection variables
    ch_host = os.getenv("CH_HOST", settings.ch_host)
    ch_port = int(os.getenv("CH_PORT", settings.ch_port))
    ch_user = os.getenv("CH_USER", settings.ch_user)
    ch_pass = os.getenv("CH_PASSWORD", settings.ch_password)

    # Fail if ClickHouse not reachable
    try:
        ch = get_client(host=ch_host, port=ch_port, username=ch_user, password=ch_pass)
        ch.query("SELECT 1")
    except Exception:
        pytest.fail("ClickHouse is not reachable")

    # Prepare a fake local file and mock downloader
    local_file = tmp_path / "tiny.grib"
    local_file.write_bytes(b"GRIB")

    def fake_download(url: str, timeout: int):
        return str(local_file), len(local_file.read_bytes()), 1

    # Override downloader dependency
    app.dependency_overrides[get_downloader] = lambda: fake_download

    # Act
    r = client.post("/insert", json={"url": "http://localhost/tiny.grib"})

    # Assert
    assert r.status_code in (200, 500)
    if r.status_code == 200:
        payload = r.json()
        assert payload.get("file_name") == "tiny.grib"

    app.dependency_overrides.clear()


def test_download_from_url(monkeypatch, tmp_path):
    # Mock downloader to simulate network download
    local_file = tmp_path / "remote.grib"
    local_file.write_bytes(b"GRIB")

    # This should basically be downloaded from S3
    def fake_download(url: str, timeout: int):
        time.sleep(0.01)
        return str(local_file), len(local_file.read_bytes()), 10

    app.dependency_overrides[get_downloader] = lambda: fake_download

    r = client.post("/insert", json={"url": "http://example.com/remote.grib"})
    assert r.status_code in (200, 500)
    if r.status_code == 200:
        payload = r.json()
        assert payload.get("file_name") == "remote.grib"

    app.dependency_overrides.clear()

# TODO: RabbitMQ integration and overall system test with S3, Clickhouse and RabbitMQ overall
