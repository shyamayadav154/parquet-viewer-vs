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
    data = {"sample_rows": "2", "include_stats": "true"}
    files = {"file": ("test.parquet", make_parquet_bytes(), "application/octet-stream")}
    r = client.post("/api/upload", data=data, files=files)
    assert r.status_code == 200, r.text
    js = r.json()
    assert js["num_rows"] == 3
    assert js["num_columns"] == 2
    assert len(js["sample"]) == 2
    assert any(f["name"] == "a" for f in js["schema"])
