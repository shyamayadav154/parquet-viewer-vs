# Parquet Viewer

Lightweight web UI (FastAPI + Tailwind CDN) to inspect Parquet files: schema, sample rows, and basic column stats.

## Features
- Upload a parquet file from browser
- Display schema (name, type, nullable)
- Show configurable number of sample rows
- Basic stats: numeric (min/max/mean/nulls) and non-numeric (unique/nulls)
- Simple JSON API endpoints (`/api/upload` & `/api/preview`)

## Quick Start

### 1. Create & activate virtualenv (optional)
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies
```bash
pip install -e .[dev]
```

### 3. Run server
```bash
uvicorn parquet_viewer.main:app --reload
```
Open: http://127.0.0.1:8000

## API
### POST /api/upload
Multipart form fields:
- `file`: parquet file
- `sample_rows` (int, default 50)
- `include_stats` (bool, default true)

Returns JSON with schema, sample rows, stats.

### GET /api/preview
Query params:
- `path`: server-side parquet file path
- `limit` (default 50)
- `offset` (default 0)
- `columns` comma separated include list

## Dev
Run tests:
```bash
pytest -q
```

Format / lint:
```bash
ruff check .
black .
```

## Notes
Tailwind is loaded via CDN (no build step). For production bundling, you can extract used classes and self-host.
