# backend/scripts/import_inmet_data.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import psycopg2
import os
import logging

# Configura o sistema de logging para uma saída mais clara
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. CONFIGURAÇÃO ---
# ATENÇÃO: A string de conexão contém dados sensíveis.
# É altamente recomendável usar variáveis de ambiente em um projeto real.
# Ex: CONNECTION_STRING = os.getenv("DATABASE_URL")
CONNECTION_STRING = "postgresql://bd_arbovirose_user:cKDg5cWiNxkIfrGAVX5242NpLhJEaqdO@dpg-d2f0hn8dl3ps73e2a1l0-a.oregon-postgres.render.com/bd_arbovirose"

# Lista de arquivos a serem processados.
# Os caminhos completos dos arquivos CSV foram definidos abaixo.
# Por favor, ajuste se a localização dos arquivos mudar.
FILES_TO_PROCESS = [
    {
        "path": r"C:\Users\ja\Desktop\MESTRADO\SCRIPT\dados_A537_D_2020-08-06_2025-08-06.csv",
        "source_name": "INMET-Diamantina-A537"
    },
    {
        "path": r"C:\Users\ja\Desktop\MESTRADO\SCRIPT\dados_A527_D_2020-08-06_2025-08-06.csv",
        "source_name": "INMET-TeofiloOtoni-A527"
    }
]

# --- 2. FUNÇÃO DE PROCESSAMENTO DO CSV ---
def process_inmet_csv(file_path: str, source_name: str) -> pd.DataFrame | None:
    """
    Carrega e processa um arquivo CSV do INMET para corresponder ao esquema do banco de dados.
    """
    logging.info(f"Processando arquivo: {file_path}")
    try:
        # O CSV do INMET tem 10 linhas de cabeçalho. Ajuste se necessário.
        # Usa ';' como separador, ',' como decimal e codificação 'latin-1'.
        df = pd.read_csv(file_path, sep=';', skiprows=10, encoding='latin-1', decimal=',')
    except FileNotFoundError:
        logging.error(f"ERRO: Arquivo não encontrado em '{file_path}'. Verifique o caminho.")
        return None
    except Exception as e:
        logging.error(f"ERRO ao ler o arquivo {file_path}: {e}")
        return None

    # Remove espaços em branco do início e fim dos nomes das colunas
    df.columns = df.columns.str.strip()
    
    # Mapeamento exato das colunas do CSV para os nomes da tabela do BD
    # Usei o cabeçalho que você forneceu para garantir a precisão
    rename_map = {
        'Data Medicao': 'date',
        'PRECIPITACAO TOTAL, DIARIO (AUT)(mm)': 'precipitation',
        'PRESSAO ATMOSFERICA MEDIA DIARIA (AUT)(mB)': 'pressure',
        'TEMPERATURA MEDIA, DIARIA (AUT)(°C)': 'temperature_mean',
        'TEMPERATURA MAXIMA, DIARIA (AUT)(°C)': 'temperature_max',
        'TEMPERATURA MINIMA, DIARIA (AUT)(°C)': 'temperature_min',
        'UMIDADE RELATIVA DO AR, MEDIA DIARIA (AUT)(%)': 'humidity_mean',
        'UMIDADE RELATIVA DO AR, MINIMA DIARIA (AUT)(%)': 'humidity_min',
        'VENTO, VELOCIDADE MEDIA DIARIA (AUT)(m/s)': 'wind_speed',
        # As colunas abaixo não são usadas na inserção no banco de dados, então não são mapeadas.
        # 'TEMPERATURA DO PONTO DE ORVALHO MEDIA DIARIA (AUT)(°C)': 'dew_point_temp',
        # 'VENTO, RAJADA MAXIMA DIARIA (AUT)(m/s)': 'wind_gust_speed'
    }
    
    # Renomeia as colunas do DataFrame usando o mapeamento
    df.rename(columns=rename_map, inplace=True)
    
    # Adiciona a coluna de origem dos dados
    df['source'] = source_name
    
    # Seleciona apenas as colunas necessárias para a tabela.
    # Note que humidity_max não está presente no arquivo CSV, mas humidity_min sim.
    # O SQL de inserção foi ajustado para refletir isso.
    final_columns = ['date', 'precipitation', 'pressure', 'temperature_mean', 'temperature_max', 'temperature_min', 'humidity_mean', 'humidity_min', 'wind_speed', 'source']
    
    # Filtra as colunas para garantir que existam no DataFrame após o renomeamento
    df = df.reindex(columns=final_columns, fill_value=0.0)

    # Converte a coluna 'date' para o formato de data
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date

    # Converte colunas numéricas, transformando erros de conversão em Nulo (NaN)
    numeric_cols = [col for col in final_columns if col not in ['date', 'source']]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Substitui valores nulos por 0.0 para garantir a inserção no BD
    df.fillna(0.0, inplace=True)
    
    logging.info(f"Arquivo '{os.path.basename(file_path)}' processado com sucesso. {len(df)} linhas encontradas.")
    return df

# --- 3. FUNÇÃO DE INSERÇÃO NO BANCO DE DADOS ---
def insert_data_to_db(df: pd.DataFrame, conn_string: str):
    """
    Insere os dados de um DataFrame na tabela climate_data.
    """
    if df is None or df.empty:
        logging.warning("Nenhum dado para inserir.")
        return

    conn = None
    try:
        logging.info("Conectando ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        logging.info("Conexão bem-sucedida. Iniciando inserção de dados...")
        
        insert_count = 0
        # Itera sobre cada linha do DataFrame para inserção
        for row in df.itertuples(index=False):
            # Inserção com controle de conflito. Usamos ON CONFLICT para evitar duplicatas.
            # O SQL foi ajustado para incluir 'humidity_min' e remover 'humidity_max',
            # que não está presente no arquivo.
            sql_query = """
                INSERT INTO climate_data (
                    date, precipitation, pressure, temperature_mean, temperature_max,
                    temperature_min, humidity_mean, humidity_min, wind_speed, source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO NOTHING;
            """
            cur.execute(sql_query, row)
            insert_count += cur.rowcount

        conn.commit()
        logging.info(f"Inserção concluída. {insert_count} novas linhas foram adicionadas para a fonte '{df['source'].iloc[0]}'.")

    except psycopg2.Error as e:
        logging.error(f"ERRO de banco de dados: {e}")
        if conn:
            conn.rollback()  # Desfaz a transação em caso de erro
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

# --- 4. EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    logging.info("Iniciando script de importação de dados climáticos.")
    for file_info in FILES_TO_PROCESS:
        processed_df = process_inmet_csv(file_info["path"], file_info["source_name"])
        if processed_df is not None:
            insert_data_to_db(processed_df, CONNECTION_STRING)
    logging.info("Script finalizado.")
