# src/ingestion/ingestor.py
import pandas as pd
import numpy as np
import logging
import os
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text # Importar 'text' para comandos SQL brutos
from config import RAW_DATA_PATH
from src.utils.db_utils import get_db_engine

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_data_to_db(file_name: str = "transactions.csv", table_name: str = "transactions"):
    """
    Lê dados de um arquivo CSV e os ingere em uma tabela de banco de dados PostgreSQL.
    Trunca a tabela antes da ingestão para garantir um estado limpo a cada execução.
    Remove duplicatas de 'transaction_id' antes da inserção para evitar UniqueViolation.

    Args:
        file_name (str): Nome do arquivo CSV a ser lido na pasta RAW_DATA_PATH.
        table_name (str): Nome da tabela no banco de dados para onde os dados serão ingeridos.
    """
    logging.info(f"Iniciando a ingestão de dados do arquivo '{file_name}' para a tabela '{table_name}'...")

    file_path = os.path.join(RAW_DATA_PATH, file_name)

    if not os.path.exists(file_path):
        logging.error(f"Arquivo não encontrado: {file_path}. Certifique-se de que os dados foram gerados.")
        return

    try:
        logging.info(f"Lendo dados do arquivo CSV: {file_path}")
        df = pd.read_csv(file_path,
                         parse_dates=['transaction_date', 'transaction_time'],
                         date_format={'transaction_date': '%Y-%m-%d', 'transaction_time': '%H:%M:%S'})

        df['transaction_date'] = df['transaction_date'].dt.date
        df['transaction_time'] = df['transaction_time'].dt.time
        
        for col in ['transaction_type', 'merchant_name', 'category', 'status']:
            if col in df.columns:
                df[col] = df[col].replace({np.nan: None})
        
        logging.info(f"'{len(df)}' registros lidos do CSV.")

        # --- NOVO PASSO: Remover duplicatas baseadas na chave primária (transaction_id) ---
        initial_rows = len(df)
        df.drop_duplicates(subset=['transaction_id'], keep='first', inplace=True)
        # keep='first' mantém a primeira ocorrência da duplicata.
        
        if len(df) < initial_rows:
            logging.warning(f"Foram removidas {initial_rows - len(df)} linhas duplicadas com base em 'transaction_id'.")
            logging.warning("Essas duplicatas foram geradas intencionalmente para fins de teste de qualidade de dados.")
        # ---------------------------------------------------------------------------------

        engine = get_db_engine()

        # --- Truncar a tabela antes de inserir ---
        logging.info(f"Truncando a tabela '{table_name}' para garantir um estado limpo...")
        with engine.connect() as connection:
            # Adicionado CASCADE para garantir que quaisquer dependências de chave estrangeira também sejam truncadas.
            connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))
            connection.commit()
        logging.info(f"Tabela '{table_name}' truncada com sucesso.")

        logging.info(f"Escrevendo dados no banco de dados na tabela '{table_name}'...")
        df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000)
        
        logging.info(f"Dados do arquivo '{file_name}' ingeridos com sucesso na tabela '{table_name}'.")

    except FileNotFoundError:
        logging.error(f"Erro: O arquivo '{file_path}' não foi encontrado.")
    except SQLAlchemyError as e:
        logging.error(f"Erro de banco de dados durante a ingestão: {e}")
        logging.error("Verifique as permissões do usuário do banco de dados e a estrutura da tabela.")
        logging.error(f"Detalhes do erro SQL: {e.orig}")
    except pd.errors.EmptyDataError:
        logging.warning(f"O arquivo CSV '{file_name}' está vazio ou mal formatado.")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado durante a ingestão de dados: {e}")

if __name__ == "__main__":
    ingest_data_to_db(file_name="transactions.csv", table_name="transactions")