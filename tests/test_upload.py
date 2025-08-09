import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient
from parquet_viewer import create_app


def make_parquet_bytes():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_upload_parquet():
    client = TestClient(create_app())
    data = {"include_stats": "true"}
    files = {"file": ("test.parquet", make_parquet_bytes(), "application/octet-stream")}
    r = client.post("/api/upload", data=data, files=files)
    assert r.status_code == 200, r.text
    js = r.json()
    assert js["num_rows"] == 3
    assert js["num_columns"] == 2
    # All rows are now returned
    assert len(js["sample"]) == 3
    assert any(f["name"] == "a" for f in js["schema"])


def test_sql_query():
    client = TestClient(create_app())
    # Upload file first to populate CURRENT_DF
    files = {"file": ("test.parquet", make_parquet_bytes(), "application/octet-stream")}
    r = client.post("/api/upload", data={"include_stats": "false"}, files=files)
    assert r.status_code == 200, r.text
    # Execute SQL
    q = {"query": "SELECT a, b FROM data WHERE a > 1", "limit": 10}
    r2 = client.post("/api/sql", json=q)
    assert r2.status_code == 200, r2.text
    js = r2.json()
    assert js["row_count"] == 2
    assert js["columns"] == ["a", "b"]
    assert len(js["rows"]) == 2
