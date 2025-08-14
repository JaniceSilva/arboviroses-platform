# backend/app.py
import os
import sqlite3
import logging
import unicodedata
from typing import List, Dict, Any

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ----------------- Paths e setup -----------------
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "arboviroses.db")

STATIC_DIR = os.path.join(BASE_DIR, "static")
DASHBOARD_INDEX = os.path.join(STATIC_DIR, "dashboard", "index.html")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("arboviroses")

# ----------------- App -----------------
app = FastAPI(title="Arboviroses API")

# /static -> serve assets do front
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ----------------- Utilidades -----------------
def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _slug(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().replace(" ", "_")


def _table_columns(conn: sqlite3.Connection, table: str) -> set:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _resolve_city(conn: sqlite3.Connection, raw_city: str) -> str:
    """Casa 'teofilo_otoni' com 'Teófilo Otoni' etc."""
    target = _slug(raw_city)
    cur = conn.execute("SELECT DISTINCT city FROM weekly_cases")
    for (c,) in cur.fetchall():
        if _slug(c) == target:
            return c
    return raw_city  # se não achou, devolve original (pode já estar em slug)


def query_weekly_cases(city: str) -> List[Dict[str, Any]]:
    """Retorna [{date, cases, temp}] robusto a variação de colunas."""
    if not os.path.exists(DB_PATH):
        logger.warning("DB não encontrado em %s", DB_PATH)
        return []

    conn = get_db_connection()
    try:
        cols = _table_columns(conn, "weekly_cases")
        if "date" not in cols:
            logger.error("Tabela weekly_cases sem coluna 'date'. Colunas: %s", cols)
            return []

        cases_col = (
            "cases" if "cases" in cols else ("total_cases" if "total_cases" in cols else None)
        )
        if not cases_col:
            logger.error("Nenhuma coluna de casos ('cases'/'total_cases') encontrada. Colunas: %s", cols)
            return []

        has_temp = "temp" in cols
        city_db = _resolve_city(conn, city)

        sql = f"""
            SELECT
              date AS date,
              {cases_col} AS cases
              {", temp AS temp" if has_temp else ", NULL AS temp"}
            FROM weekly_cases
            WHERE city = ?
            ORDER BY date
        """
        rows = conn.execute(sql, (city_db,)).fetchall()
        return [{"date": r["date"], "cases": r["cases"], "temp": r["temp"]} for r in rows]
    except Exception as e:
        logger.exception("Falha em query_weekly_cases(%s): %s", city, e)
        # devolve vazio (evita 500 no front)
        return []
    finally:
        conn.close()


def list_cities() -> List[str]:
    if not os.path.exists(DB_PATH):
        return []
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT DISTINCT city FROM weekly_cases ORDER BY city").fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        logger.exception("Falha em list_cities: %s", e)
        return []
    finally:
        conn.close()


# ----------------- Endpoints -----------------
@app.get("/")
def root():
    """Entrega o dashboard se existir; senão, mensagem simples."""
    if os.path.exists(DASHBOARD_INDEX):
        return FileResponse(DASHBOARD_INDEX)
    return {"status": "ok", "message": "Arboviroses API online (sem dashboard/index.html)"}


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/cities")
def get_cities():
    return list_cities()


@app.get("/api/data/{city}")
def get_data(city: str):
    data = query_weekly_cases(city)
    return {"data": data}

@app.get("/api/build-info")
def build_info():
    import os, glob, time
    from .app import STATIC_DIR  # ou ajuste o caminho se necessário
    assets = sorted(glob.glob(os.path.join(STATIC_DIR, "dashboard", "assets", "index-*.js")))
    ts = os.path.getmtime(assets[-1]) if assets else None
    return {
        "asset_js": os.path.basename(assets[-1]) if assets else None,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else None
    }


# --------- /api/predict (naive para não quebrar o front) ---------
class PredictRequest(BaseModel):
    city: str
    last_weeks: int = 12


@app.post("/api/predict")
def predict(req: PredictRequest):
    """
    Previsão simples (naive): repete o último valor conhecido 'last_weeks' vezes.
    Serve como fallback enquanto o modelo LSTM não está plugado.
    """
    series = query_weekly_cases(req.city)
    if not series:
        return {"prediction_weeks": []}

    last = series[-1].get("cases") or 0
    try:
        n = max(1, int(req.last_weeks))
    except Exception:
        n = 12
    preds = [last for _ in range(n)]
    return {"prediction_weeks": preds}
