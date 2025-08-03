# src/reporting/reporter.py
import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from src.utils.db_utils import get_db_engine

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_quality_reports(log_table: str = "data_validation_logs", processed_table: str = "transactions_processed"):
    """
    Gera relatórios de qualidade de dados e métricas dos dados processados,
    imprimindo-os no console para simular um dashboard.

    Args:
        log_table (str): Nome da tabela de logs de validação.
        processed_table (str): Nome da tabela de dados processados.
    """
    logging.info("Iniciando a geração de relatórios de qualidade de dados e métricas...")
    engine = get_db_engine()

    try:
        with engine.connect() as connection:
            # --- Relatório 1: Resumo das Violações de Qualidade de Dados ---
            logging.info("Gerando resumo das violações de qualidade de dados...")
            quality_violations_query = text(f"""
                SELECT
                    validation_rule,
                    COUNT(*) AS total_violations
                FROM {log_table}
                GROUP BY validation_rule
                ORDER BY total_violations DESC;
            """)
            
            violations_df = pd.read_sql(quality_violations_query, connection)

            if not violations_df.empty:
                logging.info("\n--- RESUMO DE VIOLAÇÕES DE QUALIDADE DE DADOS ---")
                logging.info(violations_df.to_string(index=False)) # to_string para formatação no log
                logging.info(f"Total de violações registradas: {violations_df['total_violations'].sum()}")
            else:
                logging.info("Nenhuma violação de qualidade de dados registrada nos logs.")

            # --- Relatório 2: Métricas Essenciais dos Dados Processados ---
            logging.info("Gerando métricas essenciais dos dados processados...")
            
            # Contagem total de transações processadas
            total_transactions_query = text(f"SELECT COUNT(*) FROM {processed_table};")
            total_transactions = connection.execute(total_transactions_query).scalar()
            logging.info(f"Total de transações processadas: {total_transactions}")

            if total_transactions > 0:
                # Soma total dos valores de transação
                total_amount_query = text(f"SELECT SUM(amount) FROM {processed_table};")
                total_amount = connection.execute(total_amount_query).scalar()
                logging.info(f"Valor total de transações processadas: R$ {total_amount:,.2f}")

                # Top 5 categorias por valor
                top_categories_query = text(f"""
                    SELECT category, SUM(amount) AS total_amount
                    FROM {processed_table}
                    GROUP BY category
                    ORDER BY total_amount DESC
                    LIMIT 5;
                """)
                top_categories_df = pd.read_sql(top_categories_query, connection)
                logging.info("\n--- TOP 5 CATEGORIAS POR VALOR DE TRANSAÇÃO ---")
                logging.info(top_categories_df.to_string(index=False, formatters={'total_amount': '{:,.2f}'.format}))

                # Número de transações por status
                transactions_by_status_query = text(f"""
                    SELECT status, COUNT(*) AS num_transactions
                    FROM {processed_table}
                    GROUP BY status
                    ORDER BY num_transactions DESC;
                """)
                transactions_by_status_df = pd.read_sql(transactions_by_status_query, connection)
                logging.info("\n--- NÚMERO DE TRANSAÇÕES POR STATUS ---")
                logging.info(transactions_by_status_df.to_string(index=False))

            else:
                logging.info("Nenhum dado na tabela processada para gerar métricas.")


    except SQLAlchemyError as e:
        logging.error(f"Erro de banco de dados durante a geração de relatórios: {e}")
        logging.error(f"Detalhes do erro SQL: {e.orig}")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado durante a geração de relatórios: {e}")
    finally:
        logging.info("Geração de relatórios concluída.")

if __name__ == "__main__":
    generate_quality_reports()