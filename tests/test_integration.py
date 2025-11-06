from functools import wraps
import os
from pathlib import Path
import shutil
import pytest

from clickhouse_connect import get_client
from fastapi.testclient import TestClient

from src.main import app
from src.infrastructure.service_provider import \
    get_downloader, get_testing_settings, get_db_service


client = TestClient(app)
settings = get_testing_settings()

def convert_to_test_db(func):
    """
    Decorator for replacing the main DB to testing DB while running tests
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Saving the name of the base DB
        temp_db_name = settings.ch_database
        # Replacing the name of the database while testing
        settings.ch_database = settings.ch_test
        
        try:
            # Running the test function
            return func(*args, **kwargs)
        finally:
            # Returning to the base DB
            settings.ch_database = temp_db_name
            
    return wrapper

"""
Added the letter 'z', because PyTest launches tests alphabetically.
And it would be better for this test to run after all the integration tests.
"""
def test_zconnection_to_main_db(): 
    # Testing the connection to main DB without changing the data
    db = get_db_service()

    # Asserting the connection object
    assert db.client != None
    # Checking if the DB name is not the same as the testing DB
    assert settings.ch_database != settings.ch_test
    # Asserting the DB name
    assert db.client.database == settings.ch_database

    # Disconnectig from DB
    db.disconnect()

@convert_to_test_db
def test_insert_into_clickhouse(monkeypatch, tmp_path):
    # Getting db service from provider
    db = get_db_service()

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

    # Prepare a local testing file and mock downloader
    source_file = Path(__file__).parent / "sample.grib2"
    local_file = tmp_path / "sample.grib2"
    shutil.copy2(source_file, local_file)

    def fake_download(url: str, timeout: int):
        return str(local_file), len(local_file.read_bytes()), 1

    # Override downloader dependency
    app.dependency_overrides[get_downloader] = lambda: fake_download

    # Act
    r = client.post("/insert", json={"url": "http://localhost/sample.grib2"})

    # Assert
    assert r.status_code == 200
    payload = r.json()
    assert payload.get("file_name") == "sample.grib2"

    # Remove testing data after insertion
    db.clear_data()
    db.disconnect()

    app.dependency_overrides.clear()

def test_download_from_url(monkeypatch, tmp_path):
    # Getting downloader from provider
    download_file = get_downloader()

    # Downloading the file from testing S3
    path, size, ms = download_file(settings.url_test, 120)

    # Basic assertion of the parameters
    assert size > 0
    assert ms > 0
    assert "forecast_" in path

    # Removing temp file
    try:
        os.remove(path)
    except Exception as e:
        print(f"Cannot delete temp file: {e}")

# TODO: RabbitMQ integration and overall system test with S3, Clickhouse and RabbitMQ
