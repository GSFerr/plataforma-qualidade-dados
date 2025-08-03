# src/validation/validator.py
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from src.utils.db_utils import get_db_engine

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_data_validations(table_name: str = "transactions", log_table: str = "data_validation_logs"):
    """
    Executa um conjunto de regras de validação nos dados da tabela especificada
    e registra as violações na tabela de logs de validação.

    Args:
        table_name (str): Nome da tabela onde os dados serão validados (ex: 'transactions').
        log_table (str): Nome da tabela onde os logs de validação serão armazenados.
    """
    logging.info(f"Iniciando a validação de dados para a tabela '{table_name}'...")
    engine = get_db_engine()

    try:
        with engine.connect() as connection:
            # Limpar logs anteriores para uma nova execução limpa
            logging.info(f"Limpando logs anteriores na tabela '{log_table}'...")
            connection.execute(text(f"TRUNCATE TABLE {log_table} RESTART IDENTITY;"))
            connection.commit()
            logging.info("Logs anteriores limpos com sucesso.")

            # --- Regra de Validação 1: Verificar valores nulos em 'amount' ---
            logging.info("Executando validação: Verificar nulos na coluna 'amount'...")
            null_amount_query = text(f"""
                SELECT transaction_id
                FROM {table_name}
                WHERE amount IS NULL;
            """)
            null_amounts = connection.execute(null_amount_query).fetchall()
            for row in null_amounts:
                logging.warning(f"Validação falhou: 'amount' é nulo para transaction_id='{row.transaction_id}'.")
                insert_log_query = text(f"""
                    INSERT INTO {log_table} (transaction_id, validation_rule, error_message)
                    VALUES (:transaction_id, :rule_name, :error_msg);
                """)
                connection.execute(insert_log_query, {
                    "transaction_id": row.transaction_id,
                    "rule_name": "Amount Nulo",
                    "error_msg": "Valor da transação (amount) é nulo."
                })
            logging.info(f"Regra 'Amount Nulo' concluída. {len(null_amounts)} violações encontradas.")
            connection.commit() # Commit após cada regra para ver logs intermediários, ou um único commit no final

            # --- Regra de Validação 2: Verificar valores nulos em 'account_id' ---
            logging.info("Executando validação: Verificar nulos na coluna 'account_id'...")
            null_account_query = text(f"""
                SELECT transaction_id
                FROM {table_name}
                WHERE account_id IS NULL;
            """)
            null_accounts = connection.execute(null_account_query).fetchall()
            for row in null_accounts:
                logging.warning(f"Validação falhou: 'account_id' é nulo para transaction_id='{row.transaction_id}'.")
                insert_log_query = text(f"""
                    INSERT INTO {log_table} (transaction_id, validation_rule, error_message)
                    VALUES (:transaction_id, :rule_name, :error_msg);
                """)
                connection.execute(insert_log_query, {
                    "transaction_id": row.transaction_id,
                    "rule_name": "Account ID Nulo",
                    "error_msg": "ID da conta (account_id) é nulo."
                })
            logging.info(f"Regra 'Account ID Nulo' concluída. {len(null_accounts)} violações encontradas.")
            connection.commit()


            # --- Regra de Validação 3: Verificar valores negativos em 'amount' ---
            logging.info("Executando validação: Verificar valores negativos na coluna 'amount'...")
            negative_amount_query = text(f"""
                SELECT transaction_id, amount
                FROM {table_name}
                WHERE amount < 0;
            """)
            negative_amounts = connection.execute(negative_amount_query).fetchall()
            for row in negative_amounts:
                logging.warning(f"Validação falhou: 'amount' negativo ({row.amount}) para transaction_id='{row.transaction_id}'.")
                insert_log_query = text(f"""
                    INSERT INTO {log_table} (transaction_id, validation_rule, error_message)
                    VALUES (:transaction_id, :rule_name, :error_msg);
                """)
                connection.execute(insert_log_query, {
                    "transaction_id": row.transaction_id,
                    "rule_name": "Amount Negativo",
                    "error_msg": f"Valor da transação (amount) é negativo: {row.amount}."
                })
            logging.info(f"Regra 'Amount Negativo' concluída. {len(negative_amounts)} violações encontradas.")
            connection.commit()

            # --- Regra de Validação 4: Verificar datas futuras em 'transaction_date' ---
            logging.info("Executando validação: Verificar datas futuras na coluna 'transaction_date'...")
            future_date_query = text(f"""
                SELECT transaction_id, transaction_date
                FROM {table_name}
                WHERE transaction_date > CURRENT_DATE;
            """)
            future_dates = connection.execute(future_date_query).fetchall()
            for row in future_dates:
                logging.warning(f"Validação falhou: 'transaction_date' é futura ({row.transaction_date}) para transaction_id='{row.transaction_id}'.")
                insert_log_query = text(f"""
                    INSERT INTO {log_table} (transaction_id, validation_rule, error_message)
                    VALUES (:transaction_id, :rule_name, :error_msg);
                """)
                connection.execute(insert_log_query, {
                    "transaction_id": row.transaction_id,
                    "rule_name": "Data Futura",
                    "error_msg": f"Data da transação (transaction_date) está no futuro: {row.transaction_date}."
                })
            logging.info(f"Regra 'Data Futura' concluída. {len(future_dates)} violações encontradas.")
            connection.commit()
            
            # --- Regra de Validação 5: Verificar categorias inválidas (exemplo de consistência de domínio) ---
            logging.info("Executando validação: Verificar categorias inválidas na coluna 'category'...")
            valid_categories = ['ALIMENTACAO', 'COMPRAS', 'SERVICOS', 'TRANSPORTES', 'SAUDE', 'LAZER']
            # Para simular uma falha aqui, vamos supor que uma categoria nova "OUTROS" ou "DESCONHECIDO" apareça
            # ou que algum valor null/empty string não seja tratado. Nosso gerador só gera categorias válidas,
            # então esta regra pode não encontrar nada a menos que modifiquemos o gerador para criar categorias inválidas.
            # No entanto, a estrutura está pronta para quando dados "reais" surgirem.
            invalid_category_query = text(f"""
                SELECT transaction_id, category
                FROM {table_name}
                WHERE category NOT IN ({', '.join([f"'{c}'" for c in valid_categories])})
                OR category IS NULL;
            """)
            invalid_categories = connection.execute(invalid_category_query).fetchall()
            for row in invalid_categories:
                logging.warning(f"Validação falhou: 'category' inválida ('{row.category}') para transaction_id='{row.transaction_id}'.")
                insert_log_query = text(f"""
                    INSERT INTO {log_table} (transaction_id, validation_rule, error_message)
                    VALUES (:transaction_id, :rule_name, :error_msg);
                """)
                connection.execute(insert_log_query, {
                    "transaction_id": row.transaction_id,
                    "rule_name": "Categoria Inválida",
                    "error_msg": f"Categoria da transação (category) é inválida: '{row.category}'."
                })
            logging.info(f"Regra 'Categoria Inválida' concluída. {len(invalid_categories)} violações encontradas.")
            connection.commit()


    except SQLAlchemyError as e:
        logging.error(f"Erro de banco de dados durante a validação de dados: {e}")
        logging.error("Verifique a conexão com o banco de dados e a estrutura das tabelas.")
        logging.error(f"Detalhes do erro SQL: {e.orig}")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado durante a validação de dados: {e}")
    finally:
        logging.info("Validação de dados concluída.")

if __name__ == "__main__":
    # Exemplo de uso ao executar o módulo diretamente
    run_data_validations()