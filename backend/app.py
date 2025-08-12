"""
FastAPI backend for the arboviroses dashboard.

This version aligns the data API with the weekly pre‑processing used during
LSTM training and introduces a lightweight SQLite database to optimise
data access.  The database is built from the raw SINAN arboviroses
dataset via the ``scripts/build_database.py`` script.  When available,
endpoints will read from ``arboviroses.db`` instead of parsing CSVs on
each request.

Endpoints:

- ``GET /api/cities`` – returns a list of available municipalities.
- ``GET /api/data/{city}`` – returns weekly aggregated case counts for a
  municipality.
- ``POST /api/predict`` – returns predictions from a pre‑trained LSTM
  model stored in ``backend/models``.  Uses the most recent ``n`` weeks of
  data for the selected city, falling back to zeros if insufficient
  history is available.

The original CSV loading functions are retained as a fallback when the
database is absent.  To generate the database, run
``python scripts/build_database.py`` from the ``backend`` folder.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import pandas as pd
import numpy as np
import joblib
import os
import sqlite3
from typing import List, Dict, Any, Optional

app = FastAPI(title="Arboviroses API")

# ---------------------------------------------------------------------------
# CORS configuration
#
# Allow the static frontend hosted on Render and local development URLs.  The
# environment variable ``ALLOWED_ORIGINS`` may contain a comma‑separated list
# of additional origins.  Each origin is stripped of whitespace before being
# applied to the middleware.
# ---------------------------------------------------------------------------
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "https://arboviroses-platform-front.onrender.com,http://localhost:5173,http://localhost:3000",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Directory constants
#
# ``BASE_DIR`` – absolute path to this file's directory.
# ``DATA_DIR`` – contains input CSVs and the SQLite database.
# ``MODEL_DIR`` – contains the pre‑trained LSTM model and associated metadata.
# ``STATIC_DIR`` – optional static assets for hosting a dashboard.
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
MODEL_DIR = os.path.join(BASE_DIR, "models")
STATIC_DIR = os.path.join(BASE_DIR, "static")
DASHBOARD_INDEX = os.path.join(STATIC_DIR, "dashboard", "index.html")
DB_PATH = os.path.join(DATA_DIR, "arboviroses.db")

# Mount static files if a directory exists.  This allows a SPA built from
# the frontend to be served by the same FastAPI process without a separate
# web server.
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def load_city_csv(city: str) -> pd.DataFrame:
    """Load a per‑city CSV from ``DATA_DIR``.

    The CSV must contain a ``date`` column.  If the file does not exist,
    an empty DataFrame is returned.
    """
    path = os.path.join(DATA_DIR, f"{city}.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Tenta encontrar coluna de data
    date_col = None
    for col in ["date", "data"]:
        if col in df.columns:
            date_col = col
            break
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df


def get_db_connection() -> sqlite3.Connection:
    """Open a connection to the SQLite database.

    If the database file does not exist, the connection will still be
    established but queries will fail until the schema is created.  It
    is the caller's responsibility to close the connection.
    """
    return sqlite3.connect(DB_PATH)


def query_weekly_cases(city: str) -> List[Dict[str, Any]]:
    """Fetch weekly case counts from the database for a given city.

    Returns a list of dictionaries with ``date`` and ``total_cases`` keys.
    If the database or table is missing, falls back to an empty list.
    """
    if not os.path.exists(DB_PATH):
        return []
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            "SELECT date, total_cases FROM weekly_cases WHERE city = ? ORDER BY date",
            (city,),
        )
        rows = cursor.fetchall()
        return [
            {"date": row[0], "total_cases": int(row[1]) if row[1] is not None else 0}
            for row in rows
        ]
    except Exception:
        # Table does not exist or other error
        return []
    finally:
        conn.close()


def get_available_cities() -> List[str]:
    """Return the list of municipalities known to the system.

    Preference is given to the SQLite database; if it exists and
    contains the ``weekly_cases`` table, distinct city names are read
    from it.  Otherwise, per‑city CSV filenames in ``DATA_DIR`` are
    inspected.  A fallback list is returned when no data sources are found.
    """
    # Attempt to read from the database
    if os.path.exists(DB_PATH):
        conn = get_db_connection()
        try:
            cursor = conn.execute("SELECT DISTINCT city FROM weekly_cases ORDER BY city")
            rows = cursor.fetchall()
            if rows:
                return [row[0] for row in rows]
        except Exception:
            pass
        finally:
            conn.close()
    # Fall back to CSV filenames
    if os.path.isdir(DATA_DIR):
        cities = [f[:-4] for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
        if cities:
            return sorted(cities)
    # Ultimate fallback
    return ["teofilo_otoni", "diamantina"]


# ---------------------------------------------------------------------------
# FastAPI endpoints
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    """Serve the dashboard HTML if present, otherwise return a status message."""
    if os.path.exists(DASHBOARD_INDEX):
        return FileResponse(DASHBOARD_INDEX)
    return {"status": "ok", "message": "Arboviroses API online (sem dashboard/index.html)"}


@app.get("/api/health")
def health():
    """Health check endpoint."""
    return {"ok": True}


@app.get("/api/cities")
def get_cities():
    """Return the list of available municipalities.

    This endpoint first checks the SQLite database for distinct city
    names; if none are present, it inspects the CSV directory.  If
    neither source yields results, it returns a hard‑coded fallback.
    """
    return sorted(get_available_cities())


@app.get("/api/data/{city}")
def get_data(city: str):
    """Return weekly aggregated data for a municipality.

    When the database is available, weekly case counts are read from the
    ``weekly_cases`` table.  Otherwise, a per‑city CSV is loaded and
    returned as originally implemented.  The returned structure is
    always a list of dictionaries with at least a ``date`` key.
    """
    # Prefer database if available
    records = query_weekly_cases(city)
    if records:
        return {"data": records}
    # Fallback to CSV
    df = load_city_csv(city)
    if df.empty:
        return {"error": "no data", "data": []}
    df = df.sort_values("date")
    return {"data": df.to_dict(orient="records")}


class PredictRequest(BaseModel):
    city: str
    last_weeks: int = 12


@app.post("/api/predict")
def predict(req: PredictRequest):
    """Return predictions for a city using the trained LSTM model.

    The endpoint attempts to retrieve the last ``req.last_weeks`` of
    weekly case counts from the database.  If the database is not
    available or lacks data for the city, it falls back to loading
    per‑city CSV files.  Zero padding is applied when fewer than
    ``last_weeks`` observations are available.
    """
    # Retrieve sequence from database
    seq: List[float] = []
    records = query_weekly_cases(req.city)
    if records:
        # Extract the total_cases values, ensure order by date ascending
        seq = [float(r.get("total_cases", 0)) for r in records]
    else:
        # Fallback to CSV
        df = load_city_csv(req.city)
        if df.empty:
            return {"error": "no data"}
        if "cases" in df.columns:
            seq = df.sort_values("date")["cases"].astype(float).values.tolist()
        elif "total_cases" in df.columns:
            seq = df.sort_values("date")["total_cases"].astype(float).values.tolist()
        else:
            return {"error": "no cases column"}
    # Pad or truncate the sequence
    if len(seq) < req.last_weeks:
        pad = [0.0] * (req.last_weeks - len(seq))
        seq = pad + seq
    else:
        seq = seq[-req.last_weeks:]
    model_path_h5 = os.path.join(MODEL_DIR, "model.h5")
    model_path_keras = os.path.join(MODEL_DIR, "model.keras")
    metadata_path = os.path.join(MODEL_DIR, "metadata.json")
    scaler_path = os.path.join(MODEL_DIR, "scaler.pkl")
    # Load model and scaler if available
    predictions: List[float]
    try:
        # Determine model file extension
        model_file = None
        if os.path.exists(model_path_h5):
            model_file = model_path_h5
        elif os.path.exists(model_path_keras):
            model_file = model_path_keras
        if model_file:
            import tensorflow as tf  # imported here to avoid dependency at startup
            scaler: Optional[Any] = None
            if os.path.exists(scaler_path):
                scaler = joblib.load(scaler_path)
            last = np.array(seq, dtype=float)
            # Apply scaling if scaler exists
            if scaler is not None:
                last_scaled = scaler.transform(last.reshape(-1, 1)).flatten()
            else:
                last_scaled = last
            # Reshape for LSTM [batch, timesteps, features]
            inp = last_scaled.reshape(1, req.last_weeks, 1)
            model = tf.keras.models.load_model(model_file)
            p_scaled = model.predict(inp).flatten()
            # Inverse scale
            if scaler is not None:
                predictions = scaler.inverse_transform(p_scaled.reshape(-1, 1)).flatten().tolist()
            else:
                predictions = p_scaled.tolist()
            predictions = [max(0.0, float(x)) for x in predictions]
        else:
            # No trained model found; default to mean of last 4 weeks
            avg = float(np.mean(seq[-4:])) if len(seq) >= 4 else float(np.mean(seq))
            predictions = [max(0.0, round(avg))] * 4
    except Exception:
        # On any error, default to mean of last 4 weeks
        avg = float(np.mean(seq[-4:])) if len(seq) >= 4 else float(np.mean(seq))
        predictions = [max(0.0, round(avg))] * 4
    return {"prediction_weeks": predictions}