#!/usr/bin/env python3
"""
Script para Extração e Inserção de Dados do SINAN sobre Arboviroses
===================================================================

Este script extrai dados reais do SINAN (Sistema de Informação de Agravos de Notificação)
dos últimos 5 anos para os municípios de Teófilo Otoni e Diamantina (MG) e insere na
tabela 'raw_cases' do banco de dados PostgreSQL.

Autor: Manus AI
Data: Agosto 2025
Fonte de dados: API InfoDengue (info.dengue.mat.br)
"""

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import io
from datetime import datetime
import os
import sys

# Dicionário de configurações do banco de dados, alinhado com o script de setup.
DB_CONFIG = {
    'host': 'dpg-d2f0hn8dl3ps73e2a1l0-a.oregon-postgres.render.com',
    'database': 'bd_arbovirose',
    'user': 'bd_arbovirose_user',
    'password': 'cKDg5cWiNxkIfrGAVX5242NpLhJEaqdO',
    'port': 5432
}

# Códigos IBGE para os municípios
MUNICIPALITIES = {
    "Teófilo Otoni": "3168606",
    "Diamantina": "3121605"
}

# Tipos de doenças (arboviroses)
DISEASES = ["dengue", "zika", "chikungunya"]

def get_sinan_data(geocode, disease, start_year, end_year):
    """
    Extrai dados do SINAN via API InfoDengue para um município e doença específicos.

    Args:
        geocode (str): Código IBGE do município
        disease (str): Tipo de doença (dengue, zika, chikungunya)
        start_year (int): Ano de início da consulta
        end_year (int): Ano de fim da consulta

    Returns:
        pandas.DataFrame: DataFrame com os dados extraídos
    """
    base_url = "https://info.dengue.mat.br/api/alertcity"
    all_data = []

    for year in range(start_year, end_year + 1):
        params = {
            "geocode": geocode,
            "disease": disease,
            "format": "csv",
            "ew_start": 1,
            "ew_end": 53,
            "ey_start": year,
            "ey_end": year
        }
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            df = pd.read_csv(io.StringIO(response.text))
            if not df.empty:
                all_data.append(df)
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados para {geocode} no ano {year}: {e}")
            continue
        except pd.errors.EmptyDataError:
            print(f"Nenhum dado retornado para {geocode} no ano {year}.")
            continue

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def get_db_ids(conn):
    """
    Busca os IDs dos municípios e da fonte de dados no banco.
    A função insere a fonte 'SINAN' se ela não existir.
    """
    cursor = conn.cursor()
    ids = {}

    # Busca os IDs dos municípios
    for name in MUNICIPALITIES.keys():
        cursor.execute("SELECT id FROM cities WHERE name = %s;", (name,))
        result = cursor.fetchone()
        if result:
            ids[name] = result[0]
        else:
            print(f"❌ Erro: Município '{name}' não encontrado na tabela 'cities'.")
            sys.exit(1)

    # Insere e busca o ID da fonte 'SINAN'
    cursor.execute("INSERT INTO sources (name) VALUES ('SINAN') ON CONFLICT (name) DO NOTHING;")
    cursor.execute("SELECT id FROM sources WHERE name = 'SINAN';")
    ids['sinan_source_id'] = cursor.fetchone()[0]

    return ids

def extract_all_data():
    """
    Extrai todos os dados para os municípios e doenças especificados.

    Returns:
        pandas.DataFrame: DataFrame consolidado com todos os dados
    """
    current_year = datetime.now().year
    start_year = current_year - 5
    end_year = current_year

    all_municipal_data = {}

    for municipality_name, geocode in MUNICIPALITIES.items():
        print(f"Coletando dados para {municipality_name} (IBGE: {geocode})...")
        municipal_data = {}
        for disease in DISEASES:
            print(f"  - {disease.capitalize()}...")
            df = get_sinan_data(geocode, disease, start_year, end_year)
            municipal_data[disease] = df
        all_municipal_data[municipality_name] = municipal_data

    # Processar e consolidar os dados
    final_data = []

    for municipality_name, data_by_disease in all_municipal_data.items():
        # Encontrar todas as semanas epidemiológicas e anos únicos
        all_se_years = set()
        for disease, df in data_by_disease.items():
            if not df.empty:
                for _, row in df.iterrows():
                    all_se_years.add((row["data_iniSE"], row["SE"]))

        for date_str, se_val in sorted(list(all_se_years)):
            row_data = {
                'date': date_str,
                'municipality': municipality_name,
                'dengue': 0,
                'zika': 0,
                'chik': 0,
                'other': 0,
            }

            for disease in DISEASES:
                df = data_by_disease[disease]
                if not df.empty:
                    filtered_df = df[(df["data_iniSE"] == date_str) & (df["SE"] == se_val)]
                    if not filtered_df.empty:
                        cases = filtered_df['casos'].sum()
                        row_data[disease] = cases
            final_data.append(row_data)

    # Criar DataFrame final
    output_df = pd.DataFrame(final_data)
    output_df['date'] = pd.to_datetime(output_df['date'])
    output_df = output_df.sort_values(by=['date', 'municipality']).reset_index(drop=True)

    return output_df

def insert_data_to_postgresql(df):
    """
    Insere os dados do DataFrame na tabela 'raw_cases' do PostgreSQL.

    Args:
        df (pandas.DataFrame): DataFrame com os dados a serem inseridos
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Obter os IDs de chave estrangeira
        db_ids = get_db_ids(conn)
        sinan_source_id = db_ids['sinan_source_id']

        # Preparar os dados para inserção na tabela 'raw_cases'
        data_tuples = []
        for _, row in df.iterrows():
            city_id = db_ids.get(row['municipality'])
            if city_id:
                data_tuples.append((
                    city_id,
                    row['date'].date(),
                    int(row['dengue']),
                    int(row['zika']),
                    int(row['chik']),
                    None, # 'other' casos
                    sinan_source_id,
                ))

        if not data_tuples:
            print("❌ Nenhum dado válido para inserção. Verifique se os municípios existem no banco.")
            return

        # Query de inserção
        insert_query = """
        INSERT INTO raw_cases
        (city_id, date, dengue, zika, chik, other, source_id)
        VALUES %s
        ON CONFLICT (city_id, date) DO NOTHING;
        """

        # Executar inserção em lote
        execute_values(cursor, insert_query, data_tuples, template=None, page_size=100)
        conn.commit()

        print(f"✅ Inseridos {len(data_tuples)} registros na tabela 'raw_cases'.")

    except psycopg2.Error as e:
        print(f"❌ Erro ao inserir dados no PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    """Função principal do script."""
    print("=" * 60)
    print("Script de Extração e Inserção de Dados do SINAN sobre Arboviroses")
    print("=" * 60)
    print(f"Municípios: {list(MUNICIPALITIES.keys())}")
    print(f"Doenças: {DISEASES}")
    print(f"Período: Últimos 5 anos (até {datetime.now().year})")
    print("=" * 60)

    try:
        # Extrair dados
        print("\n1. Extraindo dados da API InfoDengue...")
        df = extract_all_data()

        if df.empty:
            print("❌ Nenhum dado foi extraído. Verifique a conectividade e os parâmetros.")
            sys.exit(1)

        print(f"✅ Extraídos {len(df)} registros.")

        # Salvar em CSV para backup
        csv_filename = f"sinan_arboviroses_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_filename, index=False)
        print(f"📁 Dados salvos em: {csv_filename}")

        # Inserir no PostgreSQL
        print("\n2. Inserindo dados no PostgreSQL...")
        insert_data_to_postgresql(df)

        print("\n✅ Processo concluído com sucesso!")
        print("\nResumo dos dados extraídos:")
        print(f"  - Total de registros: {len(df)}")
        print(f"  - Período: {df['date'].min().date()} a {df['date'].max().date()}")
        print(f"  - Municípios: {df['municipality'].unique().tolist()}")
        print(f"  - Total de casos de dengue: {df['dengue'].sum()}")
        print(f"  - Total de casos de zika: {df['zika'].sum()}")
        print(f"  - Total de casos de chikungunya: {df['chik'].sum()}")
        print(f"  - Total geral de casos: {df[['dengue', 'zika', 'chik']].sum().sum()}")

    except Exception as e:
        print(f"❌ Erro durante a execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
