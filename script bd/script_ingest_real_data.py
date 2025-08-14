# backend/scripts/ingest_real_data.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import sys
from typing import Iterable, Dict, List, Tuple

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


# ----------------------------
# Util
# ----------------------------
def weekify(df: pd.DataFrame, date_col: str, agg: Dict[str, str]) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col])
    d["week"] = d[date_col].dt.to_period("W-SUN").dt.start_time
    out = d.groupby("week", as_index=False).agg(agg).rename(columns={"week": "date"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out


def get_conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("ERRO: set DATABASE_URL", file=sys.stderr)
        sys.exit(2)
    return psycopg2.connect(url)


# ----------------------------
# DB helpers
# ----------------------------
def fetch_cities(conn) -> pd.DataFrame:
    q = "SELECT id, name, slug, ibge_code, lat, lon FROM cities ORDER BY id"
    return pd.read_sql(q, conn)


def upsert_weather_weekly(conn, city_id: int, wk: pd.DataFrame):
    if wk.empty:
        return
    rows = [(city_id, d, float(wk.loc[i, "temp"]) if pd.notna(wk.loc[i, "temp"]) else None,
             float(wk.loc[i, "prec"]) if pd.notna(wk.loc[i, "prec"]) else None,
             float(wk.loc[i, "umid"]) if pd.notna(wk.loc[i, "umid"]) else None)
            for i, d in enumerate(wk["date"])]
    sql = """
    INSERT INTO weather_weekly (city_id, date, temp, prec, umid)
    VALUES %s
    ON CONFLICT (city_id, date) DO UPDATE SET
      temp = COALESCE(EXCLUDED.temp, weather_weekly.temp),
      prec = COALESCE(EXCLUDED.prec, weather_weekly.prec),
      umid = COALESCE(EXCLUDED.umid, weather_weekly.umid);
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


def upsert_weekly_cases(conn, city_id: int, wk: pd.DataFrame):
    if wk.empty:
        return
    rows = [(city_id, d, int(wk.loc[i, "cases"]), int(wk.loc[i, "cases"]))
            for i, d in enumerate(wk["date"])]
    sql = """
    INSERT INTO weekly_cases (city_id, date, cases, total_cases)
    VALUES %s
    ON CONFLICT (city_id, date) DO UPDATE SET
      cases = EXCLUDED.cases,
      total_cases = COALESCE(EXCLUDED.total_cases, weekly_cases.total_cases);
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


# ----------------------------
# Weather: Open-Meteo (free, sem token)
# ----------------------------
def fetch_weather_open_meteo(lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    # daily means/sums
    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start}&end_date={end}"
        "&daily=temperature_2m_mean,precipitation_sum,relative_humidity_2m_mean"
        "&timezone=auto"
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    j = r.json()
    if "daily" not in j or not j["daily"].get("time"):
        return pd.DataFrame(columns=["date", "temp", "prec", "umid"])

    daily = pd.DataFrame({
        "date": j["daily"]["time"],
        "temp": j["daily"].get("temperature_2m_mean"),
        "prec": j["daily"].get("precipitation_sum"),
        "umid": j["daily"].get("relative_humidity_2m_mean"),
    })
    # weekly aggregation
    wk = weekify(daily, "date", {"temp": "mean", "prec": "sum", "umid": "mean"})
    return wk


# ----------------------------
# Casos: OpenDataSUS (CKAN) OU CSV local
# ----------------------------
CKAN_SEARCH = "https://opendatasus.saude.gov.br/api/3/action/package_search"


def _download_csv(url: str) -> pd.DataFrame:
    with requests.get(url, timeout=120, stream=True) as r:
        r.raise_for_status()
        buf = io.BytesIO(r.content)
    # detecta separador
    head = buf.getvalue()[:4096]
    sep = ";" if head.count(b";") > head.count(b",") else ","
    buf.seek(0)
    return pd.read_csv(buf, sep=sep, low_memory=False)


def _try_ckan_find_resources(query_terms: List[str]) -> List[str]:
    """Busca recursos CSV que provavelmente tenham agregação por município/tempo."""
    q = " ".join(query_terms)
    r = requests.get(CKAN_SEARCH, params={"q": q, "rows": 20}, timeout=60)
    r.raise_for_status()
    out = []
    data = r.json().get("result", {}).get("results", [])
    for pkg in data:
        for res in pkg.get("resources", []):
            fmt = (res.get("format") or "").lower()
            name = (res.get("name") or "").lower()
            if fmt in ("csv", "text/csv") and any(k in name for k in ["semana", "município", "municipio", "se", "serie", "série"]):
                if res.get("url"):
                    out.append(res["url"])
    return out


def load_cases_from_opendatasus(ibge_code: str, start_date: str) -> pd.DataFrame:
    """
    Tenta baixar séries de Dengue/Zika/Chik do OpenDataSUS.
    Como os conjuntos mudam ao longo do tempo, buscamos CSVs 'prováveis' e conciliamos.
    """
    urls = []
    # 3 buscas que costumam cobrir arboviroses no SINAN
    urls += _try_ckan_find_resources(["SINAN", "Dengue", "município", "semana"])
    urls += _try_ckan_find_resources(["SINAN", "Zika", "município", "semana"])
    urls += _try_ckan_find_resources(["SINAN", "Chikungunya", "município", "semana"])
    if not urls:
        # Sem garantia — usuário pode informar manualmente com --cases-csv
        return pd.DataFrame(columns=["date", "cases"])

    frames = []
    for u in urls:
        try:
            df = _download_csv(u)
        except Exception:
            continue

        cols = {c.lower(): c for c in df.columns}
        # heurísticas comuns
        mun_col = cols.get("id_municipio") or cols.get("cod_municipio") or cols.get("municipio_ibge")
        se_col  = cols.get("semana") or cols.get("semana_epidemiologica") or cols.get("se")
        ano_col = cols.get("ano") or cols.get("ano_notificacao") or cols.get("ano_epi")
        cases_col = cols.get("casos") or cols.get("confirmados") or cols.get("quantidade")

        if not (mun_col and se_col and ano_col and cases_col):
            continue

        dfx = df[[mun_col, se_col, ano_col, cases_col]].rename(
            columns={mun_col:"ibge", se_col:"se", ano_col:"ano", cases_col:"cases"}
        )
        # filtro do município
        dfx = dfx[dfx["ibge"].astype(str).str.zfill(7) == str(ibge_code).zfill(7)]
        if dfx.empty:
            continue

        # cria data (domingo da semana epidemiológica)
        # W-SUN: usamos a convenção ISO: semana começa segunda; ajustamos para domingo com to_period.
        # Para simplificar: definimos a data como o domingo da semana "se" no ano "ano".
        dfx["se"] = pd.to_numeric(dfx["se"], errors="coerce")
        dfx["ano"] = pd.to_numeric(dfx["ano"], errors="coerce")
        dfx = dfx.dropna(subset=["se", "ano"])
        dfx["se"] = dfx["se"].astype(int)
        dfx["ano"] = dfx["ano"].astype(int)

        # converte (ano, se) -> data aproximada (domingo)
        # regra: primeira semana ISO -> segunda-feira; pegamos start_time (segunda) e retrocedemos 1 dia.
        # Pequena aproximação suficiente para agregação semanal consistente no dashboard.
        iso = pd.to_datetime(dfx["ano"].astype(str) + "-W" + dfx["se"].astype(str) + "-1", errors="coerce", format="%G-W%V-%u")
        dfx["date"] = (iso - pd.to_timedelta(1, unit="D")).dt.date

        dfx["cases"] = pd.to_numeric(dfx["cases"], errors="coerce").fillna(0).astype(int)
        dfx = dfx[["date", "cases"]]
        frames.append(dfx)

    if not frames:
        return pd.DataFrame(columns=["date", "cases"])

    out = pd.concat(frames, ignore_index=True)
    # consolida — somatório por semana
    out = out.groupby("date", as_index=False)["cases"].sum()
    # recorta pelo start_date (≥5 anos)
    out = out[out["date"] >= pd.to_datetime(start_date).date()]
    return out


def load_cases_from_csv(csv_path: str, ibge_code: str, start_date: str) -> pd.DataFrame:
    # CSV genérico com colunas variáveis; tentamos deduzir.
    df = pd.read_csv(csv_path, sep=None, engine="python")
    cols = {c.lower(): c for c in df.columns}
    # tentativas usuais
    mun_col = cols.get("municipio") or cols.get("municipality") or cols.get("ibge") or cols.get("cod_municipio")
    date_col = cols.get("date") or cols.get("data") or cols.get("data_notificacao") or cols.get("dt_notificacao")
    cases_col = cols.get("cases") or cols.get("casos") or cols.get("total_cases") or cols.get("quantidade")

    if not (mun_col and date_col and cases_col):
        raise ValueError(f"CSV {csv_path} precisa conter colunas de município, data e casos.")

    d = df[[mun_col, date_col, cases_col]].rename(columns={mun_col:"ibge", date_col:"date", cases_col:"cases"})
    # aceita ibge, nome ou slug
    d["ibge"] = d["ibge"].astype(str)
    d = d[ (d["ibge"].str.zfill(7) == str(ibge_code).zfill(7)) | (d["ibge"].str.contains(str(ibge_code))) ]
    if d.empty:
        return pd.DataFrame(columns=["date","cases"])

    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date"])
    d["cases"] = pd.to_numeric(d["cases"], errors="coerce").fillna(0).astype(int)

    wk = weekify(d, "date", {"cases":"sum"})
    wk = wk[wk["date"] >= pd.to_datetime(start_date).date()]
    return wk


# ----------------------------
# CLI principal
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Ingestão de clima e casos (≥5 anos) para PostgreSQL.")
    ap.add_argument("--start", default=None, help="Data inicial (YYYY-MM-DD). Padrão: hoje-5 anos")
    ap.add_argument("--end", default=None, help="Data final (YYYY-MM-DD). Padrão: hoje")
    ap.add_argument("--cases-source", choices=["opendatasus", "csv"], default="opendatasus")
    ap.add_argument("--cases-csv", help="Se --cases-source=csv, caminho do CSV.")
    ap.add_argument("--cities", nargs="*", help="Lista de slugs para limitar (ex.: teofilo_otoni diamantina)")
    args = ap.parse_args()

    end = args.end or dt.date.today().strftime("%Y-%m-%d")
    start = args.start or (dt.date.today() - dt.timedelta(days=365*5 + 10)).strftime("%Y-%m-%d")

    conn = get_conn()
    cities = fetch_cities(conn)
    if args.cities:
        cities = cities[cities["slug"].isin(args.cities)]
    if cities.empty:
        print("Sem cidades na tabela `cities`. Insira antes de rodar a ingestão.", file=sys.stderr)
        sys.exit(2)

    print(f"Ingestão de {len(cities)} cidade(s) - janela {start} → {end}")

    # --- clima
    for _, row in cities.iterrows():
        if pd.isna(row["lat"]) or pd.isna(row["lon"]):
            print(f"[clima] pulando {row['name']} (sem lat/lon).")
            continue
        wk_weather = fetch_weather_open_meteo(row["lat"], row["lon"], start, end)
        upsert_weather_weekly(conn, int(row["id"]), wk_weather)
        print(f"[clima] {row['name']}: {len(wk_weather)} semanas carregadas.")

    # --- casos
    for _, row in cities.iterrows():
        print(f"[casos] {row['name']}...")
        if args.cases_source == "csv":
            if not args.cases_csv:
                print("Faltou --cases-csv", file=sys.stderr); sys.exit(2)
            wk_cases = load_cases_from_csv(args.cases_csv, str(row["ibge_code"]), start)
        else:
            wk_cases = load_cases_from_opendatasus(str(row["ibge_code"]), start)

        # Se veio diário, wk_cases já deve estar semanal; se veio semanal, também está.
        if "date" in wk_cases and "cases" in wk_cases:
            upsert_weekly_cases(conn, int(row["id"]), wk_cases)
            print(f"[casos] {row['name']}: {len(wk_cases)} semanas carregadas.")
        else:
            print(f"[casos] {row['name']}: nenhuma linha encontrada (verifique fonte).")

    conn.close()
    print("✅ Ingestão concluída.")


if __name__ == "__main__":
    main()
