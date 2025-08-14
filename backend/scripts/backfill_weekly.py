# -*- coding: utf-8 -*-
"""
Backfill semanal (casos e clima) para o SQLite da plataforma.

- Lê CSV(s) de casos e clima, normaliza colunas e agrega por semana (W-SUN).
- Atualiza/insere nas tabelas:
    weekly_cases(city, date, cases, total_cases)
    weather_weekly(city, date, temp, prec, umid)
- Faz migração de schema automaticamente (mantém compatibilidade).
- UPSERT com ON CONFLICT(city, date) DO UPDATE (sem duplicar PK).

Uso:
    # autodetecta CSVs em backend/data
    python backend/scripts/backfill_weekly.py

    # CSV único de clima
    python backend/scripts/backfill_weekly.py --weather backend/data/teofilo_otoni.csv

    # vários CSVs/clima ou pasta com CSVs
    python backend/scripts/backfill_weekly.py --weather "backend/data"
    python backend/scripts/backfill_weekly.py --weather "backend/data/teofilo_otoni.csv,backend/data/diamantina.csv"

    # passando o de casos explicitamente
    python backend/scripts/backfill_weekly.py --cases backend/data/sinan_arboviroses_data.csv
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path
from typing import Iterable, List

import pandas as pd

# --- caminhos base -----------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]  # raiz do repo
DATA_DIR = ROOT / "backend" / "data"
DB_PATH = DATA_DIR / "arboviroses.db"


# --- utilidades --------------------------------------------------------------
def _infer_sep(path: Path) -> str:
    """Tenta detectar ; ou , automaticamente."""
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        head = f.read(4096)
    return ";" if head.count(";") > head.count(",") else ","


def _read_csv_any(path: Path) -> pd.DataFrame:
    """Lê CSV com auto-separador e decimal adequado (INMET usa ; e ,)."""
    sep = _infer_sep(path)
    kw = {"sep": sep}
    if sep == ";":
        kw["decimal"] = ","  # números como 12,3
    try:
        return pd.read_csv(path, **kw)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1", **kw)


def _norm_cols(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    m = {c: mapping[c.lower()] for c in df.columns.str.lower() if c.lower() in mapping}
    return df.rename(columns=m)


def _norm_city_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace("_", " ")
        .str.title()
    )


def _parse_dates(series: pd.Series) -> pd.Series:
    d = pd.to_datetime(series, errors="coerce", dayfirst=False)
    if d.isna().mean() > 0.5:
        d = pd.to_datetime(series, errors="coerce", dayfirst=True)
    if d.isna().any():
        s2 = series.astype(str).str.replace(r"[^\d\-W]", "", regex=True)
        d2 = pd.to_datetime(s2, errors="coerce")
        d = d.fillna(d2)
    return d


def weekify(
    df: pd.DataFrame,
    date_col: str = "date",
    sum_cols: tuple[str, ...] = (),
    mean_cols: tuple[str, ...] = (),
    city_col: str = "city",
) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col])
    d["week"] = d[date_col].dt.to_period("W-SUN").dt.start_time  # domingo
    agg = {}
    for c in sum_cols:
        if c in d.columns:
            agg[c] = "sum"
    for c in mean_cols:
        if c in d.columns:
            agg[c] = "mean"
    out = d.groupby([city_col, "week"], as_index=False).agg(agg)
    out = out.rename(columns={"week": "date"})
    out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
    return out


# --- carregadores ------------------------------------------------------------
def load_cases_csv(path: Path) -> pd.DataFrame:
    df = _read_csv_any(path)
    df = _norm_cols(
        df,
        {
            "municipio": "city",
            "município": "city",
            "cidade": "city",
            "city": "city",
            "date": "date",
            "data": "date",
            "semana": "date",
            "casos": "cases",
            "total_cases": "cases",
            "cases": "cases",
            "notificacoes": "cases",
        },
    )
    need = {"city", "date", "cases"}
    if not need.issubset(set(df.columns.str.lower())):
        raise ValueError(
            "CSV de casos precisa conter colunas equivalentes a city/date/cases "
            f"(recebi: {list(df.columns)})"
        )
    df["city"] = _norm_city_series(df["city"])
    df["date"] = _parse_dates(df["date"])
    df = df.dropna(subset=["date"])
    df["cases"] = pd.to_numeric(df["cases"], errors="coerce").fillna(0).astype(int)
    return df[["city", "date", "cases"]]


def _canon(s: str) -> str:
    import unicodedata, re

    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _city_from_filename(path: Path) -> str:
    return (
        path.stem.replace("_", " ").replace("-", " ").strip().title()
    )  # teofilo_otoni -> Teofilo Otoni


def load_weather_csv(path: Path) -> pd.DataFrame:
    """
    Aceita:
      - CSV com colunas 'city' e 'date', OU
      - CSV por cidade (sem 'city'): cidade é inferida pelo nome do arquivo.

    Detecta colunas por palavras-chave (evita 'ponto de orvalho' para temp).
    """
    df = _read_csv_any(path)

    # mapeamento direto simples (se existir)
    df = _norm_cols(
        df,
        {
            "municipio": "city",
            "município": "city",
            "cidade": "city",
            "city": "city",
            "date": "date",
            "data": "date",
            "temp": "temp",
            "temperatura": "temp",
            "tmed": "temp",
            "prec": "prec",
            "chuva": "prec",
            "prcp": "prec",
            "umid": "umid",
            "umidade": "umid",
            "ur": "umid",
        },
    )

    # se não tem city/date, fazemos detecção por nome
    lower = {c: _canon(c) for c in df.columns}

    # data
    date_col = None
    for orig, low in lower.items():
        if any(k in low for k in ["data", "date"]):
            date_col = orig
            break
    if date_col is None:
        raise ValueError(f"Não encontrei coluna de data em {path.name}.")

    # temperatura do ar (não usar 'orvalho')
    temp_col = None
    for orig, low in lower.items():
        if any(k in low for k in ["temperatura", "tmed", "temp"]):
            if "orvalho" not in low:
                temp_col = orig
                break

    # precipitação
    prec_col = None
    for orig, low in lower.items():
        if any(k in low for k in ["precipitacao", "chuva", "prcp", "(mm)"]):
            prec_col = orig
            break

    # umidade relativa
    umid_col = None
    for orig, low in lower.items():
        if any(k in low for k in ["umidade", "umid", "ur", "%)"]):
            umid_col = orig
            break

    out = pd.DataFrame()
    out["date"] = _parse_dates(df[date_col])
    out = out.dropna(subset=["date"])

    if temp_col and temp_col in df.columns:
        out["temp"] = pd.to_numeric(df[temp_col], errors="coerce")
    if prec_col and prec_col in df.columns:
        out["prec"] = pd.to_numeric(df[prec_col], errors="coerce")
    if umid_col and umid_col in df.columns:
        out["umid"] = pd.to_numeric(df[umid_col], errors="coerce")

    # city: do arquivo ou da coluna existente
    if "city" in df.columns:
        out["city"] = _norm_city_series(df["city"]) if len(df["city"].dropna()) else _city_from_filename(path)
    else:
        out["city"] = _city_from_filename(path)

    keep = ["city", "date"] + [c for c in ("temp", "prec", "umid") if c in out.columns]
    return out[keep]


# --- banco / schema / upsert -------------------------------------------------
def table_cols(con: sqlite3.Connection, table: str) -> list[str]:
    try:
        return [r[1] for r in con.execute(f"PRAGMA table_info({table})")]
    except Exception:
        return []


def ensure_schema(con: sqlite3.Connection):
    # weekly_cases
    cols = table_cols(con, "weekly_cases")
    if not cols:
        con.execute(
            """
            CREATE TABLE weekly_cases(
                city TEXT,
                date TEXT,
                cases INTEGER,
                total_cases INTEGER,
                PRIMARY KEY (city, date)
            );
            """
        )
    else:
        if "cases" not in cols:
            con.execute("ALTER TABLE weekly_cases ADD COLUMN cases INTEGER;")
            if "total_cases" in cols:
                con.execute("UPDATE weekly_cases SET cases = total_cases WHERE cases IS NULL;")
        if "total_cases" not in cols:
            con.execute("ALTER TABLE weekly_cases ADD COLUMN total_cases INTEGER;")
            if "cases" in cols:
                con.execute("UPDATE weekly_cases SET total_cases = cases WHERE total_cases IS NULL;")
    con.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_weekly_cases_city_date ON weekly_cases(city, date);"
    )

    # weather_weekly
    cols = table_cols(con, "weather_weekly")
    if not cols:
        con.execute(
            """
            CREATE TABLE weather_weekly(
                city TEXT,
                date TEXT,
                temp REAL,
                prec REAL,
                umid REAL,
                PRIMARY KEY (city, date)
            );
            """
        )
    else:
        for c, typ in (("temp", "REAL"), ("prec", "REAL"), ("umid", "REAL")):
            if c not in cols:
                con.execute(f"ALTER TABLE weather_weekly ADD COLUMN {c} {typ};")
    con.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_weekly_city_date ON weather_weekly(city, date);"
    )
    con.commit()


def upsert_weekly_cases(con: sqlite3.Connection, df: pd.DataFrame):
    if df.empty:
        return
    payload = df.copy()
    if "total_cases" not in payload.columns:
        payload["total_cases"] = payload["cases"]
    payload["date"] = pd.to_datetime(payload["date"]).dt.date.astype(str)

    sql = """
    INSERT INTO weekly_cases (city, date, cases, total_cases)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(city, date) DO UPDATE SET
        cases = excluded.cases,
        total_cases = COALESCE(excluded.total_cases, weekly_cases.total_cases);
    """
    con.executemany(
        sql,
        list(
            zip(
                payload["city"].astype(str),
                payload["date"].astype(str),
                payload["cases"].astype(int),
                payload["total_cases"].astype(int),
            )
        ),
    )
    con.commit()


def upsert_weather_weekly(con: sqlite3.Connection, df: pd.DataFrame):
    if df.empty:
        return
    cols = ["city", "date"] + [c for c in ("temp", "prec", "umid") if c in df.columns]
    payload = df[cols].copy()
    payload["date"] = pd.to_datetime(payload["date"]).dt.date.astype(str)

    set_parts = []
    for c in ("temp", "prec", "umid"):
        if c in payload.columns:
            set_parts.append(f"{c} = COALESCE(excluded.{c}, weather_weekly.{c})")
    set_sql = ", ".join(set_parts) if set_parts else "date = excluded.date"

    placeholders = ",".join(["?"] * len(cols))
    insert_cols = ",".join(cols)

    sql = f"""
    INSERT INTO weather_weekly ({insert_cols})
    VALUES ({placeholders})
    ON CONFLICT(city, date) DO UPDATE SET
        {set_sql};
    """
    con.executemany(sql, list(map(tuple, payload.astype(object).to_numpy())))
    con.commit()


# --- auto-descoberta de arquivos --------------------------------------------
def auto_find_cases() -> Path | None:
    candidates = [
        DATA_DIR / "sinan_multi_anos.csv",
        DATA_DIR / "sinan_arboviroses_data.csv",
        DATA_DIR / "real_data.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    for p in DATA_DIR.glob("*.csv"):
        if re.search(r"(sinan|caso|arbovirose)", p.name, re.I):
            return p
    return None


def _looks_like_weather(name: str) -> bool:
    return bool(re.search(r"(weather|inmet|meteo|clima|tempo|teofilo|diamantina)", name, re.I))


def auto_find_weather_files() -> List[Path]:
    files: List[Path] = []
    for p in DATA_DIR.glob("*.csv"):
        if _looks_like_weather(p.name):
            files.append(p)
    return files


def _coerce_to_paths(arg: str | None) -> List[Path]:
    if not arg:
        return []
    paths: List[Path] = []
    # pode ser lista separada por vírgula
    for token in [t.strip() for t in arg.split(",") if t.strip()]:
        p = Path(token)
        if p.is_dir():
            for f in p.glob("*.csv"):
                if _looks_like_weather(f.name):
                    paths.append(f)
        elif p.exists():
            paths.append(p)
    # remove duplicados preservando ordem
    seen = set()
    uniq = []
    for p in paths:
        if p.resolve() not in seen:
            uniq.append(p)
            seen.add(p.resolve())
    return uniq


# --- main --------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", type=str, help="CSV de casos (city,date,cases)", default=None)
    parser.add_argument(
        "--weather",
        type=str,
        help="CSV(s) de clima: arquivo, lista separada por vírgulas, ou pasta com CSVs",
        default=None,
    )
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    cases_path = Path(args.cases) if args.cases else auto_find_cases()
    if not cases_path or not cases_path.exists():
        raise SystemExit(
            "ERRO: não encontrei CSV de casos. Informe com --cases "
            "(ex.: backend/data/sinan_arboviroses_data.csv)"
        )

    weather_paths = _coerce_to_paths(args.weather)
    if not weather_paths:
        weather_paths = auto_find_weather_files()

    print(f"Banco: {DB_PATH}")
    print(f"Casos: {cases_path}")
    if weather_paths:
        print("Clima:")
        for p in weather_paths:
            print(f"  - {p}")
    else:
        print("Clima: (não informado)")

    with sqlite3.connect(DB_PATH) as con:
        ensure_schema(con)

        # ---- casos
        casos_df = load_cases_csv(cases_path)
        wk_cases = weekify(casos_df, "date", sum_cols=("cases",))
        upsert_weekly_cases(con, wk_cases)
        n_cases = con.execute("SELECT COUNT(*) FROM weekly_cases").fetchone()[0]
        print(f"✅ weekly_cases atualizado. Linhas totais: {n_cases}")

        # ---- clima (opcional; aceita vários arquivos)
        if weather_paths:
            frames = []
            for w in weather_paths:
                try:
                    frames.append(load_weather_csv(w))
                except Exception as e:
                    print(f"⚠️  Ignorando {w.name}: {e}")
            if frames:
                met_df = pd.concat(frames, ignore_index=True)
                wk_weather = weekify(
                    met_df,
                    "date",
                    sum_cols=tuple([c for c in ("prec",) if c in met_df.columns]),
                    mean_cols=tuple([c for c in ("temp", "umid") if c in met_df.columns]),
                )
                upsert_weather_weekly(con, wk_weather)
                n_w = con.execute("SELECT COUNT(*) FROM weather_weekly").fetchone()[0]
                print(f"✅ weather_weekly atualizado. Linhas totais: {n_w}")
            else:
                print("⚠️  Nenhum CSV de clima válido lido.")
        else:
            print("⚠️  Sem CSV de clima. Tabela weather_weekly não foi atualizada.")

    print("🏁 Backfill concluído com sucesso.")

    con = sqlite3.connect("backend/data/arboviroses.db")
    for row in con.execute("SELECT city, COUNT(*) FROM weekly_cases GROUP BY city;"):
        print(row)
    con.close()


if __name__ == "__main__":
    main()

