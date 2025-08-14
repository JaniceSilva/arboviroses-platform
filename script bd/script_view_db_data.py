# backend/scripts/view_db_data.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError

# Configura o sistema de logging para uma saída mais clara
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_conn():
    """
    Cria e retorna um motor de conexão (engine) do SQLAlchemy para o banco de dados PostgreSQL.
    Lê a string de conexão da variável de ambiente DATABASE_URL.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        logging.error("ERRO: A variável de ambiente DATABASE_URL não está configurada.")
        sys.exit(2)
    try:
        # A forma mais recomendada de usar pandas.read_sql() é com um engine do SQLAlchemy.
        # Isso garante melhor compatibilidade e recursos.
        engine = create_engine(url)
        return engine
    except OperationalError as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        sys.exit(2)

def fetch_and_display_data(conn):
    """
    Busca dados de tabelas específicas e os exibe no console.

    Args:
        conn: Conexão com o banco de dados (neste caso, um engine do SQLAlchemy).
    """
    # Dicionário com a ordenação correta para tabelas conhecidas
    specific_queries = {
        "cities": "SELECT * FROM cities ORDER BY id LIMIT 20;",
        "weather_weekly": "SELECT * FROM weather_weekly ORDER BY city_id, date LIMIT 20;",
        "weekly_cases": "SELECT * FROM weekly_cases ORDER BY city_id, date LIMIT 20;"
    }
    
    try:
        # Busca dinamicamente todas as tabelas no esquema 'public'
        all_tables_query = "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
        all_tables_df = pd.read_sql(all_tables_query, conn)
        all_table_names = all_tables_df['tablename'].tolist()
    except Exception as e:
        logging.error(f"Erro ao buscar a lista de tabelas: {e}")
        return

    for table in all_table_names:
        logging.info(f"Buscando dados da tabela '{table}'...")
        query = specific_queries.get(table, f"SELECT * FROM \"{table}\" LIMIT 20;")

        try:
            df = pd.read_sql(query, conn)
            print("-" * 50)
            print(f"Dados da tabela '{table}':")
            print("-" * 50)
            if df.empty:
                print("Tabela vazia.")
            else:
                # Exibe o DataFrame formatado
                print(df.to_string())
            print("\n")
        except ProgrammingError:
            logging.warning(f"A tabela '{table}' não foi encontrada ou a query falhou. Pulando.")
        except Exception as e:
            logging.error(f"Erro ao buscar dados da tabela '{table}': {e}")
            
def main():
    """
    Função principal que gerencia o fluxo de conexão e exibição dos dados.
    """
    logging.info("Iniciando a visualização dos dados do banco de dados...")
    engine = get_conn()
    if engine:
        with engine.connect() as conn:
            fetch_and_display_data(conn)
        logging.info("Conexão com o banco de dados encerrada.")
    logging.info("✅ Visualização de dados concluída.")

if __name__ == "__main__":
    main()
