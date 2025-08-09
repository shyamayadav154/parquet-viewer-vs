from fastapi.testclient import TestClient
from parquet_viewer import create_app


def test_index():
    app = create_app()
    client = TestClient(app)
    r = client.get("/")
    assert r.status_code == 200
    assert "Parquet Viewer" in r.text
