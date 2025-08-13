# --- adicione perto dos imports ---
import unicodedata
import logging
logger = logging.getLogger(__name__)

def _slug(s: str) -> str:
    if s is None: 
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().replace(" ", "_")

def _table_columns(conn, table: str) -> set:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}

def _resolve_city(conn, raw_city: str) -> str:
    """Tenta casar 'teofilo_otoni' com 'Teófilo Otoni' etc."""
    target = _slug(raw_city)
    cur = conn.execute("SELECT DISTINCT city FROM weekly_cases")
    cities = [r[0] for r in cur.fetchall()]
    for c in cities:
        if _slug(c) == target:
            return c
    # se não achou, devolve original (pode haver slug já gravado)
    return raw_city

def query_weekly_cases(city: str) -> list[dict]:
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
