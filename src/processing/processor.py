# src/processing/processor.py
import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from src.utils.db_utils import get_db_engine

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_transactions_data(source_table: str = "transactions", target_table: str = "transactions_processed"):
    """
    Lê os dados brutos da tabela de origem, aplica regras de tratamento e transformação,
    e carrega os dados processados em uma nova tabela de destino.

    Args:
        source_table (str): Nome da tabela de onde os dados brutos serão lidos.
        target_table (str): Nome da tabela onde os dados processados serão salvos.
    """
    logging.info(f"Iniciando o tratamento e transformação de dados da tabela '{source_table}' para '{target_table}'...")
    engine = get_db_engine()

    try:
        with engine.connect() as connection:
            # 1. Ler os dados da tabela bruta
            logging.info(f"Lendo dados da tabela '{source_table}'...")
            query = text(f"SELECT * FROM {source_table};")
            df = pd.read_sql(query, connection)
            logging.info(f"'{len(df)}' registros lidos da tabela '{source_table}'.")

            # 2. Aplicar Regras de Tratamento e Transformação de ALTO NÍVEL

            # a) Tratamento de Nulos: 'amount' e 'account_id'
            # Preencher 'amount' nulo com 0.00
            initial_null_amounts = df['amount'].isnull().sum()
            if initial_null_amounts > 0:
                df['amount'].fillna(0.00, inplace=True)
                logging.info(f"Tratamento: {initial_null_amounts} valores nulos na coluna 'amount' preenchidos com 0.00.")

            # Preencher 'account_id' nulo com 'UNKNOWN'
            initial_null_account_ids = df['account_id'].isnull().sum()
            if initial_null_account_ids > 0:
                df['account_id'].fillna('UNKNOWN', inplace=True)
                logging.info(f"Tratamento: {initial_null_account_ids} valores nulos na coluna 'account_id' preenchidos com 'UNKNOWN'.")

            # b) Tratamento de Valores Negativos: 'amount'
            initial_negative_amounts = (df['amount'] < 0).sum()
            if initial_negative_amounts > 0:
                df['amount'] = df['amount'].abs() # Transformar em valor absoluto
                logging.info(f"Tratamento: {initial_negative_amounts} valores negativos na coluna 'amount' convertidos para positivo (valor absoluto).")

            # c) Tratamento de Datas Futuras: 'transaction_date'
            # Convertendo para datetime para comparação, se ainda não for
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            current_date = pd.to_datetime(pd.Timestamp.now().date())
            
            initial_future_dates = (df['transaction_date'] > current_date).sum()
            if initial_future_dates > 0:
                df.loc[df['transaction_date'] > current_date, 'transaction_date'] = current_date
                logging.info(f"Tratamento: {initial_future_dates} datas futuras na coluna 'transaction_date' atualizadas para a data atual ({current_date.strftime('%Y-%m-%d')}).")
            
            # Converter de volta para date.date objects para compatibilidade com o PostgreSQL se a coluna for DATE
            df['transaction_date'] = df['transaction_date'].dt.date

            # d) Tratamento de Categorias Inválidas/Nulas
            # Se houvesse categorias inválidas (não apenas nulas), o ideal seria um mapeamento.
            # Para nulos ou strings vazias, podemos padronizar para 'OUTROS'.
            valid_categories = ['ALIMENTACAO', 'COMPRAS', 'SERVICOS', 'TRANSPORTES', 'SAUDE', 'LAZER']
            initial_invalid_categories = df['category'].isnull().sum() + (~df['category'].isin(valid_categories)).sum()
            
            # Preenche nulos com 'OUTROS'
            if df['category'].isnull().sum() > 0:
                df['category'].fillna('OUTROS', inplace=True)
                logging.info(f"Tratamento: {df['category'].isnull().sum()} valores nulos na coluna 'category' preenchidos com 'OUTROS'.")

            # Trata categorias que não estão na lista de válidas (se houver, nosso gerador não as cria)
            if (~df['category'].isin(valid_categories)).sum() > 0:
                 df.loc[~df['category'].isin(valid_categories), 'category'] = 'OUTROS'
                 logging.info(f"Tratamento: {initial_invalid_categories} valores de categorias inválidas preenchidos com 'OUTROS'.")


            logging.info("Transformações de dados aplicadas com sucesso.")

            # 3. Carregar os dados tratados para a nova tabela
            logging.info(f"Truncando a tabela '{target_table}' para garantir um estado limpo...")
            connection.execute(text(f"TRUNCATE TABLE {target_table} RESTART IDENTITY CASCADE;"))
            connection.commit()
            logging.info(f"Tabela '{target_table}' truncada com sucesso.")

            logging.info(f"Escrevendo dados processados no banco de dados na tabela '{target_table}'...")
            df.to_sql(target_table, engine, if_exists='append', index=False, chunksize=1000)
            
            logging.info(f"Dados processados salvos com sucesso na tabela '{target_table}'. Total de registros: {len(df)}")

    except SQLAlchemyError as e:
        logging.error(f"Erro de banco de dados durante o processamento de dados: {e}")
        logging.error("Verifique a conexão com o banco de dados e a estrutura das tabelas.")
        logging.error(f"Detalhes do erro SQL: {e.orig}")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado durante o processamento de dados: {e}")
    finally:
        logging.info("Tratamento e transformação de dados concluída.")

if __name__ == "__main__":
    process_transactions_data()