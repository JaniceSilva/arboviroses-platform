# backend/app.py
from __future__ import annotations

import os
import sqlite3
import logging
import unicodedata
from typing import List, Dict, Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

# -----------------------------------------------------------------------------
# Configuração básica
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ROOT_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(ROOT_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "arboviroses.db")
DASHBOARD_DIR = os.path.join(ROOT_DIR, "static", "dashboard")

app = FastAPI(title="Arboviroses API")

# CORS (mantém liberado, útil se o front for separado)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajuste se quiser restringir
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Servir o dashboard estático (se existir)
if os.path.isdir(DASHBOARD_DIR):
    app.mount("/static/dashboard", StaticFiles(directory=DASHBOARD_DIR, html=True), name="dashboard")

# Página inicial -> carrega o index do dashboard, se existir
@app.get("/", response_class=HTMLResponse)
def index():
    index_html = os.path.join(DASHBOARD_DIR, "index.html")
    if os.path.exists(index_html):
        with open(index_html, "r", encoding="utf-8") as f:
            return f.read()
    return '<h1>Arboviroses API online (sem dashboard)</h1>'

@app.get("/api/health")
def health():
    return {"status": "ok"}

# -----------------------------------------------------------------------------
# Utilitários de banco e normalização
# -----------------------------------------------------------------------------
def get_db_connection() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def _slug(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().replace(" ", "_")

def _table_columns(conn: sqlite3.Connection, table: str) -> set:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in cur.fetchall()}
    except Exception:
        return set()

def _resolve_city(conn: sqlite3.Connection, raw_city: str) -> str:
    """Casa 'teofilo_otoni' -> 'Teófilo Otoni' etc."""
    target = _slug(raw_city)
    cur = conn.execute("SELECT DISTINCT city FROM weekly_cases")
    cities = [r[0] for r in cur.fetchall()]
    for c in cities:
        if _slug(c) == target:
            return c
    return raw_city  # devolve como veio se não casar

# -----------------------------------------------------------------------------
# Query de dados semanais
# -----------------------------------------------------------------------------
def query_weekly_cases(city: str) -> List[Dict[str, Any]]:
    if not os.path.exists(DB_PATH):
        logger.warning("DB não encontrado em %s", DB_PATH)
        return []

    conn = get_db_connection()
    try:
        cols = _table_columns(conn, "weekly_cases")
        if "date" not in cols:
            logger.error("Tabela weekly_cases sem coluna 'date'")
            return []

        cases_col = "cases" if "cases" in cols else ("total_cases" if "total_cases" in cols else None)
        if not cases_col:
            logger.error("Nenhuma coluna de casos encontrada (nem 'cases' nem 'total_cases'). Colunas: %s", cols)
            return []

        has_temp = False
        # Se precisar, podemos JOIN com weather_weekly futuramente; por ora retorna só cases
        city_db = _resolve_city(conn, city)

        sql = f"""
            SELECT date AS date, {cases_col} AS cases
            FROM weekly_cases
            WHERE city = ?
            ORDER BY date
        """
        rows = conn.execute(sql, (city_db,)).fetchall()
        return [{"date": r["date"], "cases": r["cases"]} for r in rows]

    except Exception as e:
        logger.exception("Falha em query_weekly_cases(%s): %s", city, e)
        return []
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# Endpoints de API
# -----------------------------------------------------------------------------
@app.get("/api/cities")
def api_cities():
    if not os.path.exists(DB_PATH):
        return []
    with get_db_connection() as con:
        rows = con.execute("SELECT DISTINCT city FROM weekly_cases ORDER BY city").fetchall()
    return [r["city"] for r in rows]

@app.get("/api/data/{city}")
def api_data(city: str):
    data = query_weekly_cases(city)
    return {"city": city, "data": data}

@app.post("/api/predict")
def api_predict(payload: Dict[str, Any]):
    """
    Fallback simples: repete o último valor por N semanas.
    Substitua pelo carregamento do seu LSTM global quando quiser.
    """
    city = payload.get("city")
    last_weeks = int(payload.get("last_weeks", 12))
    if not city:
        raise HTTPException(status_code=400, detail="Parâmetro 'city' é obrigatório.")

    series = query_weekly_cases(city)
    if not series:
        raise HTTPException(status_code=404, detail="Sem dados históricos para a cidade.")

    last_val = int(series[-1]["cases"] or 0)
    preds = [last_val for _ in range(last_weeks)]
    return {
        "city": city,
        "model": "naive_repeat_last",
        "prediction_weeks": preds,
        "confidence": "very_low",
    }
