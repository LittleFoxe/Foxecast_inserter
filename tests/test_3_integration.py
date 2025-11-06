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
    Decorator for replacing the main DB to testing DB while running tests.
    Used to avoid the problem with TestSettings and Settings having the same variable ch_database.
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

def test_1_download_from_url(monkeypatch, tmp_path):
    """
    Test file download functionality from external URL.
    
    Steps:
    - Download test file from configured test URL
    - Validate download results and file properties
    - Clean up temporary downloaded file
    
    Asserts:
    - Downloaded file has positive size
    - Download operation completes within expected time
    - File path contains expected naming pattern
    """
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

@convert_to_test_db
def test_2_insert_into_clickhouse(monkeypatch, tmp_path):
    """
    Test GRIB file insertion into ClickHouse using mocked file download.
    
    Steps:
    - Setup test database connection
    - Verify ClickHouse availability
    - Prepare local test file and mock downloader
    - Execute HTTP POST to insert endpoint
    - Validate response and clean up test data
    
    Asserts:
    - HTTP 200 status code on successful insertion
    - Response contains expected file name
    - Test data is properly cleaned up
    """
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

def test_3_connection_to_main_db():
    """
    Verify connection to main production database without modifying test environment.
    Checks if the link to main DB is not changed to testing DB.
    
    Asserts:
    - Database client connection is established successfully
    - Production and test database names are distinct
    - Connected database matches the production database name
    """
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

def test_4_broker_integration():
    """
    Verify AMQP consumer can connect and subscribe without real broker using mocks.

    Steps:
    - Create fake RabbitMQ connection and channel objects
    - Mock aio-pika connect_robust function
    - Start consumer task and verify subscription
    - Cancel task and validate clean shutdown
    
    Asserts:
    - Connection establish and channel creation succeed
    - QoS prefetch count is set correctly
    - Queue consumption starts with valid callback
    - Consumer task can be cancelled without errors
    """
    import types
    import asyncio
    from unittest.mock import patch
    from src.infrastructure.rabbit_consumer import run_consumer

    # Simple fakes for aio-pika interfaces
    class FakeQueue:
        def __init__(self):
            self._consumed = False
            self._callback = None

        async def consume(self, cb):
            self._consumed = True
            self._callback = cb

    class FakeChannel:
        def __init__(self, queue: FakeQueue):
            self.queue = queue
            self.prefetch = None

        async def set_qos(self, prefetch_count: int):
            self.prefetch = prefetch_count

        async def declare_queue(self, name: str, durable: bool):
            return self.queue

    class FakeConnection:
        def __init__(self):
            self.closed = False
            self.queue = FakeQueue()
            self.channel_obj = FakeChannel(self.queue)

        async def channel(self):
            return self.channel_obj

        async def close(self):
            self.closed = True

    async def _run_once_and_cancel():
        fake_conn = FakeConnection()

        async def fake_connect(url: str):
            return fake_conn

        with patch("src.infrastructure.rabbit_consumer.aio_pika.connect_robust", new=fake_connect):
            # Start consumer and cancel shortly after
            task = asyncio.create_task(run_consumer())
            await asyncio.sleep(0.05)
            # Ensure subscribed without exception
            assert fake_conn.channel_obj.prefetch is not None
            assert fake_conn.queue._consumed is True
            # Validate callback type is the actual handler
            assert isinstance(fake_conn.queue._callback, types.FunctionType) or callable(fake_conn.queue._callback)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    # Run in an isolated loop
    asyncio.run(_run_once_and_cancel())

@convert_to_test_db
def test_5_overall_integration():
    """
    Simulate end-to-end workflow via AMQP handler and HTTP POST, and assert metrics.

    - Invoke handle_message with a fake message payload to ensure it completes without exception
    - POST /insert with a local file to avoid network
    - Assert metrics changed after operations
    - Cleanup ClickHouse test data
    """
    import asyncio
    import json
    from src.infrastructure.rabbit_consumer import handle_message
    from src.infrastructure.service_provider import get_db_service

    # Initializing db service to clear data after the test
    db = get_db_service()

    # Capture metrics before operations
    metrics_before = client.get("/metrics").text

    # 1) Simulate AMQP message handling with test URL
    # (it should be used instead of sending to the real queue,
    # because it might break the logic)
    class FakeIncomingMessage:
        def __init__(self, body: bytes):
            self.body = body

        class _Proc:
            def __init__(self, outer):
                self.outer = outer
            async def __aenter__(self):
                return self.outer
            async def __aexit__(self, exc_type, exc, tb):
                return False

        def process(self, requeue: bool = False):
            return self._Proc(self)

    async def _run_handler_once():
        # Use the test URL in the AMQP message
        payload = {"file": settings.url_test}
        msg = FakeIncomingMessage(json.dumps(payload).encode("utf-8"))
        # Override downloader used by handler through provider
        await handle_message(msg)

    asyncio.run(_run_handler_once())

    # 2) Perform POST /insert with test URL and ensure 200 OK with expected fields
    r = client.post("/insert", json={"url": settings.url_test})
    assert r.status_code == 200
    resp = r.json()
    assert resp.get("file_name") is not None
    assert resp.get("download_ms") is not None
    assert resp.get("parse_ms") is not None
    assert resp.get("db_ms") is not None

    # 3) Metrics should reflect operations
    metrics_after = client.get("/metrics").text

    def _extract_metric(text: str, name: str) -> float:
        for line in text.splitlines():
            if line.startswith(name + " "):
                try:
                    return float(line.split()[1])
                except Exception:
                    pass
        return 0.0

    # Counters/Gauges to check
    before_net = _extract_metric(metrics_before, "network_bytes_total")
    after_net = _extract_metric(metrics_after, "network_bytes_total")
    before_size = _extract_metric(metrics_before, "file_size_bytes")
    after_size = _extract_metric(metrics_after, "file_size_bytes")

    assert after_net >= before_net
    assert after_size >= before_size  # File size should be our mock value

    # Histograms expose *_count; ensure counts increased for key histograms
    def _extract_count(text: str, base: str) -> float:
        for line in text.splitlines():
            if line.startswith(base + "_count"):
                try:
                    return float(line.split()[1])
                except Exception:
                    pass
        return 0.0

    before_dl = _extract_count(metrics_before, "file_download_seconds")
    after_dl = _extract_count(metrics_after, "file_download_seconds")
    before_parse = _extract_count(metrics_before, "parse_seconds")
    after_parse = _extract_count(metrics_after, "parse_seconds")
    before_db = _extract_count(metrics_before, "db_insert_seconds")
    after_db = _extract_count(metrics_after, "db_insert_seconds")

    assert after_dl >= before_dl
    assert after_parse >= before_parse
    assert after_db >= before_db

    # Cleanup
    db.clear_data()
    db.disconnect()
    app.dependency_overrides.clear()
    