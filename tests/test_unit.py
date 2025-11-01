from fastapi.testclient import TestClient

from src.main import app
from src.services.db_service import DatabaseService


client = TestClient(app)
db = DatabaseService()


def test_health_ok():
    r = client.get("/health")
    assert r.status_code == 200
