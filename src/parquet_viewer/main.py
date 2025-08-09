from __future__ import annotations

import io
from pathlib import Path
from typing import List, Optional

import orjson
import pandas as pd
import pyarrow.parquet as pq
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "_uploads"
UPLOAD_DIR.mkdir(exist_ok=True)


def create_app() -> FastAPI:
    app = FastAPI(title="Parquet Viewer", version="0.1.0")

    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        return templates.TemplateResponse("index.html", {"request": request})

    @app.post("/api/upload")
    async def upload_parquet(
        file: UploadFile = File(...),
        include_stats: bool = Form(True),
    ):
        # Basic validation
        if not file.filename or not file.filename.endswith(".parquet"):
            raise HTTPException(status_code=400, detail="Only .parquet files are supported")
        content = await file.read()
        buffer = io.BytesIO(content)
        try:
            table = pq.read_table(buffer)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"Failed to read parquet: {e}") from e

        schema = [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
            }
            for field in table.schema
        ]

        df: pd.DataFrame = table.to_pandas()

        # Return all rows (may be large; consider pagination for huge files)
        sample = df.to_dict(orient="records")

        stats = None
        if include_stats:
            stats = {}
            for col in df.columns:
                series = df[col]
                if pd.api.types.is_numeric_dtype(series):
                    stats[col] = {
                        "min": float(series.min()) if not series.empty else None,
                        "max": float(series.max()) if not series.empty else None,
                        "mean": float(series.mean()) if not series.empty else None,
                        "nulls": int(series.isna().sum()),
                    }
                else:
                    stats[col] = {
                        "unique": int(series.nunique(dropna=True)),
                        "nulls": int(series.isna().sum()),
                    }

        return JSONResponse(
            content=orjson.loads(
                orjson.dumps(
                    {
                        "filename": file.filename,
                        "num_rows": len(df),
                        "num_columns": len(df.columns),
                        "schema": schema,
                        "sample": sample,
                        "stats": stats,
                    }
                )
            )
        )

    @app.get("/api/preview", response_class=JSONResponse)
    async def preview(
        path: str, limit: int = 50, offset: int = 0, columns: Optional[str] = None
    ):
        fpath = Path(path)
        if not fpath.exists():
            raise HTTPException(status_code=404, detail="File not found")
        try:
            table = pq.read_table(fpath)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(e)) from e

        cols: Optional[List[str]] = columns.split(",") if columns else None
        if cols:
            table = table.select(cols)
        df = table.to_pandas()
        return JSONResponse(
            content=orjson.loads(
                orjson.dumps(
                    {
                        "preview": df.iloc[offset : offset + limit].to_dict(orient="records"),
                        "total": len(df),
                    }
                )
            )
        )

    return app


app = create_app()
