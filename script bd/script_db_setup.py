# script_db_setup.py

import psycopg2
from urllib.parse import urlparse
import sys


# "postgresql://bd_arbovirose_user:cKDg5cWiNxkIfrGAVX5242NpLhJEaqdO@dpg-d2f0hn8dl3ps73e2a1l0-a.oregon-postgres.render.com/bd_arbovirose?sslmode=require"


sql_script = """
BEGIN;

-- 1) Dicionários
CREATE TABLE IF NOT EXISTS cities (
  id      SERIAL PRIMARY KEY,
  name    TEXT NOT NULL,
  slug    TEXT NOT NULL UNIQUE,  -- ex: teofilo_otoni
  ibge_code TEXT,
  lat     REAL,
  lon     REAL
);

CREATE TABLE IF NOT EXISTS sources (
  id    SERIAL PRIMARY KEY,
  name  TEXT NOT NULL UNIQUE,      -- ex: SINAN, INMET, ERA5
  meta  JSONB
);

-- 2) Camada RAW (dados diários, sem agregação)
CREATE TABLE IF NOT EXISTS raw_cases (
  city_id     INT NOT NULL REFERENCES cities(id),
  date        DATE NOT NULL,
  dengue      INT,
  zika        INT,
  chik        INT,
  other       INT,
  total_cases INT GENERATED ALWAYS AS (
    COALESCE(dengue,0)+COALESCE(zika,0)+COALESCE(chik,0)+COALESCE(other,0)
  ) STORED,
  source_id   INT REFERENCES sources(id),
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Correção: PRIMARY KEY não pode ser uma expressão como COALESCE.
  -- Usamos PRIMARY KEY nas colunas NOT NULL e um índice único separado para
  -- manter a lógica de unicidade desejada, incluindo o source_id.
  PRIMARY KEY (city_id, date)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_cases_unique ON raw_cases (city_id, date, COALESCE(source_id, 0));

CREATE TABLE IF NOT EXISTS raw_weather (
  city_id     INT NOT NULL REFERENCES cities(id),
  station_id  TEXT,
  date        DATE NOT NULL,
  tmed        REAL,    -- temperatura média °C
  prcp        REAL,    -- precipitação mm
  umid        REAL,    -- umidade %
  source_id   INT REFERENCES sources(id),
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Correção: PRIMARY KEY não pode ser uma expressão como COALESCE.
  -- Usamos PRIMARY KEY nas colunas NOT NULL e um índice único separado para
  -- manter a lógica de unicidade desejada, incluindo o source_id e station_id.
  PRIMARY KEY (city_id, date)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_weather_unique ON raw_weather (city_id, date, COALESCE(source_id, 0), COALESCE(station_id, ''));

-- 3) Agregação semanal (W-SUN: início do domingo)
CREATE TABLE IF NOT EXISTS weekly_cases (
  city_id     INT NOT NULL REFERENCES cities(id),
  date        DATE NOT NULL,      -- início da semana
  cases       INT NOT NULL,       -- total semanal
  total_cases INT,                -- compat. (espelho de cases)
  PRIMARY KEY (city_id, date)
);
CREATE INDEX IF NOT EXISTS idx_weekly_cases_city_date ON weekly_cases(city_id, date);

CREATE TABLE IF NOT EXISTS weather_weekly (
  city_id INT NOT NULL REFERENCES cities(id),
  date    DATE NOT NULL,
  temp    REAL,                     -- média semanal
  prec    REAL,                     -- soma semanal
  umid    REAL,                     -- média semanal
  PRIMARY KEY (city_id, date)
);
CREATE INDEX IF NOT EXISTS idx_weather_weekly_city_date ON weather_weekly(city_id, date);

-- 4) Features (para treino/servir previsões)
CREATE TABLE IF NOT EXISTS features_weekly (
  city_id    INT NOT NULL REFERENCES cities(id),
  date       DATE NOT NULL,
  cases      INT,
  mm4        REAL,                  -- média móvel 4 semanas
  temp       REAL,
  prec       REAL,
  umid       REAL,
  lag1       REAL,
  lag2       REAL,
  weekofyear INT,
  PRIMARY KEY (city_id, date)
);
CREATE INDEX IF NOT EXISTS idx_features_weekly_city_date ON features_weekly(city_id, date);

-- 5) Previsões (auditoria e leitura rápida)
CREATE TABLE IF NOT EXISTS forecasts (
  city_id     INT NOT NULL REFERENCES cities(id),
  run_at      TIMESTAMPTZ NOT NULL,
  model_name  TEXT NOT NULL,        -- ex: global_lstm:v1
  horizon     INT  NOT NULL,        -- semanas previstas
  start_date  DATE NOT NULL,
  yhat        REAL[] NOT NULL,
  lower       REAL[],
  upper       REAL[],
  metrics     JSONB,
  PRIMARY KEY (city_id, run_at, model_name)
);
CREATE INDEX IF NOT EXISTS idx_forecasts_city_start ON forecasts(city_id, start_date);

-- 6) Views de compatibilidade (para backend que espera 'city' TEXT e 'date' como string)
CREATE OR REPLACE VIEW weekly_cases_legacy AS
SELECT
  c.name AS city,
  to_char(w.date, 'YYYY-MM-DD') AS date,
  w.cases,
  COALESCE(w.total_cases, w.cases) AS total_cases
FROM weekly_cases w
JOIN cities c ON c.id = w.city_id;

CREATE OR REPLACE VIEW weather_weekly_legacy AS
SELECT
  c.name AS city,
  to_char(w.date, 'YYYY-MM-DD') AS date,
  w.temp, w.prec, w.umid
FROM weather_weekly w
JOIN cities c ON c.id = w.city_id;

-- 7) Cidades de exemplo (edite/expanda conforme seu projeto)
INSERT INTO cities (name, slug, ibge_code, lat, lon) VALUES
  ('Teófilo Otoni','teofilo_otoni','3168606',-17.857,-41.508),
  ('Diamantina','diamantina','3121606',-18.241,-43.600)
ON CONFLICT (slug) DO NOTHING;

COMMIT;
"""

# String de conexão fornecida pelo usuário
db_connection_string = "postgresql://bd_arbovirose_user:cKDg5cWiNxkIfrGAVX5242NpLhJEaqdO@dpg-d2f0hn8dl3ps73e2a1l0-a.oregon-postgres.render.com/bd_arbovirose?sslmode=require"

def setup_database():
    """Conecta ao banco de dados e executa o script SQL."""
    conn = None
    try:
        # Tenta se conectar ao banco de dados
        print("Conectando ao banco de dados...")
        conn = psycopg2.connect(db_connection_string)
        conn.autocommit = False
        cursor = conn.cursor()

        # Executa o script SQL
        print("Executando o script SQL para criar as tabelas...")
        cursor.execute(sql_script)

        # Confirma as transações
        conn.commit()
        print("✅ Banco de dados configurado com sucesso!")

    except psycopg2.Error as e:
        print(f"❌ Erro ao conectar ou executar o script SQL: {e}", file=sys.stderr)
        if conn:
            conn.rollback()  # Desfaz a transação em caso de erro
        sys.exit(1)

    finally:
        # Garante que a conexão seja fechada, mesmo se houver um erro
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    setup_database()
