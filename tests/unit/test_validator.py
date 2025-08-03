# tests/unit/test_validator.py
import pytest
import pandas as pd
from sqlalchemy import text
from src.utils.db_utils import get_db_engine
from src.validation.validator import run_data_validations
from datetime import date, time, timedelta

# --- FIXTURE PARA O ENGINE DO BANCO DE DADOS ---
@pytest.fixture(scope="module")
def db_engine():
    """Retorna um engine de SQLAlchemy para ser usado em todos os testes do módulo."""
    return get_db_engine()

# --- FIXTURE PARA LIMPEZA E INSERÇÃO DE DADOS DE TESTE ---
@pytest.fixture(scope="function")
def setup_teardown_db_data(db_engine):
    """
    Fixture para limpar as tabelas e inserir dados de teste para cada função de teste,
    e limpar novamente após a execução.
    """
    with db_engine.connect() as connection:
        # Limpeza antes de cada teste
        connection.execute(text("TRUNCATE TABLE transactions RESTART IDENTITY CASCADE;"))
        connection.execute(text("TRUNCATE TABLE data_validation_logs RESTART IDENTITY CASCADE;"))
        connection.commit()
        
        # --- NOVO: Retorna a conexão para que o teste possa inserir dados personalizados ---
        yield connection # O teste será executado aqui
        
        # Limpeza após cada teste
        connection.execute(text("TRUNCATE TABLE transactions RESTART IDENTITY CASCADE;"))
        connection.execute(text("TRUNCATE TABLE data_validation_logs RESTART IDENTITY CASCADE;"))
        connection.commit()

# --- TESTES PARA O MÓDULO VALIDATOR.PY ---

def test_validator_detects_null_amount(setup_teardown_db_data, db_engine):
    """
    Testa se o validador detecta transações com 'amount' nulo.
    """
    conn = setup_teardown_db_data # Recebe a conexão da fixture
    
    # --- NOVA IMPLEMENTAÇÃO: INSERÇÃO DE DADOS DE TESTE CONTROLADOS ---
    # Inserir dados com amount nulo
    conn.execute(text("""
        INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_time, amount, currency, transaction_type, merchant_name, category, status)
        VALUES (:id, :account, :date, :time, :amount, :currency, :type, :merchant, :category, :status);
    """), {
        "id": "trans-1",
        "account": "acc-1",
        "date": date(2025, 1, 1),
        "time": time(10, 0, 0),
        "amount": None, # Valor nulo
        "currency": "BRL",
        "type": "PURCHASE",
        "merchant": "Loja A",
        "category": "COMPRAS",
        "status": "CONCLUIDA"
    })
    conn.commit()

    # --- NOVA IMPLEMENTAÇÃO: EXECUÇÃO DO VALIDADOR ---
    run_data_validations(table_name="transactions")

    # --- NOVA IMPLEMENTAÇÃO: VERIFICAÇÃO DOS LOGS ---
    with db_engine.connect() as check_conn:
        logs_df = pd.read_sql(text("SELECT transaction_id, validation_rule, error_message FROM data_validation_logs;"), check_conn)
    
    assert not logs_df.empty, "Nenhum log de validação encontrado."
    assert "trans-1" in logs_df['transaction_id'].values, "transaction_id 'trans-1' não encontrado nos logs."
    assert "Amount Nulo" in logs_df['validation_rule'].values, "Regra 'Amount Nulo' não registrada nos logs."


def test_validator_detects_null_account_id(setup_teardown_db_data, db_engine):
    """
    Testa se o validador detecta transações com 'account_id' nulo.
    """
    conn = setup_teardown_db_data
    conn.execute(text("""
        INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_time, amount, currency, transaction_type, merchant_name, category, status)
        VALUES (:id, :account, :date, :time, :amount, :currency, :type, :merchant, :category, :status);
    """), {
        "id": "trans-2",
        "account": None, # account_id nulo
        "date": date(2025, 2, 1),
        "time": time(11, 0, 0),
        "amount": 100.00,
        "currency": "BRL",
        "type": "DEPOSIT",
        "merchant": "Banco B",
        "category": "OUTROS",
        "status": "CONCLUIDA"
    })
    conn.commit()

    run_data_validations(table_name="transactions")

    with db_engine.connect() as check_conn:
        logs_df = pd.read_sql(text("SELECT transaction_id, validation_rule, error_message FROM data_validation_logs;"), check_conn)
    
    assert "trans-2" in logs_df['transaction_id'].values
    assert "Account ID Nulo" in logs_df['validation_rule'].values


def test_validator_detects_negative_amount(setup_teardown_db_data, db_engine):
    """
    Testa se o validador detecta transações com 'amount' negativo.
    """
    conn = setup_teardown_db_data
    conn.execute(text("""
        INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_time, amount, currency, transaction_type, merchant_name, category, status)
        VALUES (:id, :account, :date, :time, :amount, :currency, :type, :merchant, :category, :status);
    """), {
        "id": "trans-3",
        "account": "acc-3",
        "date": date(2025, 3, 1),
        "time": time(12, 0, 0),
        "amount": -50.00, # amount negativo
        "currency": "BRL",
        "type": "WITHDRAWAL",
        "merchant": "Caixa C",
        "category": "FINANCAS",
        "status": "CONCLUIDA"
    })
    conn.commit()

    run_data_validations(table_name="transactions")

    with db_engine.connect() as check_conn:
        logs_df = pd.read_sql(text("SELECT transaction_id, validation_rule, error_message FROM data_validation_logs;"), check_conn)
    
    assert "trans-3" in logs_df['transaction_id'].values
    assert "Amount Negativo" in logs_df['validation_rule'].values


def test_validator_detects_future_date(setup_teardown_db_data, db_engine):
    """
    Testa se o validador detecta transações com 'transaction_date' no futuro.
    """
    conn = setup_teardown_db_data
    future_date = date.today() + timedelta(days=10) # Data no futuro
    conn.execute(text("""
        INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_time, amount, currency, transaction_type, merchant_name, category, status)
        VALUES (:id, :account, :date, :time, :amount, :currency, :type, :merchant, :category, :status);
    """), {
        "id": "trans-4",
        "account": "acc-4",
        "date": future_date, # Data futura
        "time": time(13, 0, 0),
        "amount": 200.00,
        "currency": "BRL",
        "type": "PAYMENT",
        "merchant": "Servico D",
        "category": "SERVICOS",
        "status": "PENDENTE"
    })
    conn.commit()

    run_data_validations(table_name="transactions")

    with db_engine.connect() as check_conn:
        logs_df = pd.read_sql(text("SELECT transaction_id, validation_rule, error_message FROM data_validation_logs;"), check_conn)
    
    assert "trans-4" in logs_df['transaction_id'].values
    assert "Data Futura" in logs_df['validation_rule'].values

def test_validator_detects_invalid_category(setup_teardown_db_data, db_engine):
    """
    Testa se o validador detecta transações com 'category' inválida (e.g., nula ou fora da lista).
    """
    conn = setup_teardown_db_data
    # Inserir com categoria nula (nosso validador cobre isso)
    conn.execute(text("""
        INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_time, amount, currency, transaction_type, merchant_name, category, status)
        VALUES (:id, :account, :date, :time, :amount, :currency, :type, :merchant, :category, :status);
    """), {
        "id": "trans-5",
        "account": "acc-5",
        "date": date(2025, 4, 1),
        "time": time(14, 0, 0),
        "amount": 75.00,
        "currency": "BRL",
        "type": "PURCHASE",
        "merchant": "Loja E",
        "category": None, # Categoria nula
        "status": "CONCLUIDA"
    })
    # Inserir com categoria não reconhecida
    conn.execute(text("""
        INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_time, amount, currency, transaction_type, merchant_name, category, status)
        VALUES (:id, :account, :date, :time, :amount, :currency, :type, :merchant, :category, :status);
    """), {
        "id": "trans-6",
        "account": "acc-6",
        "date": date(2025, 4, 2),
        "time": time(15, 0, 0),
        "amount": 120.00,
        "currency": "BRL",
        "type": "PURCHASE",
        "merchant": "Loja F",
        "category": "CATEGORIA_INVALIDA", # Categoria não reconhecida
        "status": "CONCLUIDA"
    })
    conn.commit()

    run_data_validations(table_name="transactions")

    with db_engine.connect() as check_conn:
        logs_df = pd.read_sql(text("SELECT transaction_id, validation_rule, error_message FROM data_validation_logs;"), check_conn)
    
    assert "trans-5" in logs_df['transaction_id'].values
    assert "trans-6" in logs_df['transaction_id'].values
    
    # Ambos devem cair na regra 'Categoria Inválida'
    assert "Categoria Inválida" in logs_df.loc[logs_df['transaction_id'] == "trans-5", 'validation_rule'].values
    assert "Categoria Inválida" in logs_df.loc[logs_df['transaction_id'] == "trans-6", 'validation_rule'].values


def test_validator_no_violations(setup_teardown_db_data, db_engine):
    """
    Testa se o validador não registra violações quando os dados são válidos.
    """
    conn = setup_teardown_db_data
    # Inserir dados totalmente válidos
    conn.execute(text("""
        INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_time, amount, currency, transaction_type, merchant_name, category, status)
        VALUES (:id, :account, :date, :time, :amount, :currency, :type, :merchant, :category, :status);
    """), {
        "id": "trans-valid-1",
        "account": "acc-valid-1",
        "date": date(2025, 5, 1),
        "time": time(9, 0, 0),
        "amount": 99.99,
        "currency": "BRL",
        "type": "PURCHASE",
        "merchant": "Loja Valid",
        "category": "COMPRAS",
        "status": "CONCLUIDA"
    })
    conn.execute(text("""
        INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_time, amount, currency, transaction_type, merchant_name, category, status)
        VALUES (:id, :account, :date, :time, :amount, :currency, :type, :merchant, :category, :status);
    """), {
        "id": "trans-valid-2",
        "account": "acc-valid-2",
        "date": date(2025, 5, 2),
        "time": time(10, 0, 0),
        "amount": 10.50,
        "currency": "USD",
        "type": "DEPOSIT",
        "merchant": "Bank Valid",
        "category": "FINANCAS",
        "status": "PENDENTE"
    })
    conn.commit()

    run_data_validations(table_name="transactions")

    with db_engine.connect() as check_conn:
        logs_df = pd.read_sql(text("SELECT * FROM data_validation_logs;"), check_conn)
    
    # Espera que a tabela de logs esteja vazia
    assert logs_df.empty, "Logs de validação encontrados quando não deveriam haver violações."