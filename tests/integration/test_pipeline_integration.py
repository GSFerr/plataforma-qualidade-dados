import pytest
import pandas as pd
import os
from sqlalchemy import text
from src.utils.db_utils import get_db_engine
from src.data_generation.generator import generate_transactions_data
from src.ingestion.ingestor import ingest_data_to_db
from src.validation.validator import run_data_validations
from config import RAW_DATA_PATH # Importa o caminho da pasta de dados brutos
import logging

# --- FIXTURE PARA O ENGINE DO BANCO DE DADOS ---
@pytest.fixture(scope="module")
def db_engine():
    """Retorna um engine de SQLAlchemy para ser usado em todos os testes do módulo."""
    return get_db_engine()

# --- FIXTURE PARA LIMPEZA DE DADOS DE TESTE E ARQUIVOS ---
@pytest.fixture(scope="function")
def setup_teardown_integration_data(db_engine):
    """
    Fixture para garantir que as tabelas do DB e os arquivos CSV sejam limpos
    antes e depois de cada teste de integração.
    """
    test_csv_file = os.path.join(RAW_DATA_PATH, "integration_transactions.csv")
    
    with db_engine.connect() as connection:
        # Limpeza do DB antes do teste
        connection.execute(text("TRUNCATE TABLE transactions RESTART IDENTITY CASCADE;"))
        connection.execute(text("TRUNCATE TABLE data_validation_logs RESTART IDENTITY CASCADE;"))
        connection.commit()
    
    # Limpeza de arquivos CSV antes do teste
    if os.path.exists(test_csv_file):
        os.remove(test_csv_file)
        
    yield # O teste será executado aqui
    
    with db_engine.connect() as connection:
        # Limpeza do DB após o teste
        connection.execute(text("TRUNCATE TABLE transactions RESTART IDENTITY CASCADE;"))
        connection.execute(text("TRUNCATE TABLE data_validation_logs RESTART IDENTITY CASCADE;"))
        connection.commit()
    
    # Limpeza de arquivos CSV após o teste
    if os.path.exists(test_csv_file):
        os.remove(test_csv_file)

# --- TESTES DE INTEGRAÇÃO ---

def test_full_ingestion_and_validation_flow(setup_teardown_integration_data, db_engine):
    """
    Testa o fluxo completo de geração, ingestão e validação de dados,
    verificando se os erros esperados são registrados nos logs.
    """
    num_records = 500 # Um número pequeno para o teste
    test_csv_filename = "integration_transactions.csv"
    
    # --- NOVA IMPLEMENTAÇÃO: Geração de dados de teste com erros conhecidos ---
    # Geramos propositalmente um cenário para ter erros esperados:
    # - Nulo em account_id e amount
    # - Negativo em amount
    # - Data futura
    # - Duplicatas de ID (o ingestor deve lidar com isso)
    # A proporção do gerador garante a injeção desses erros em 50 registros.
    generate_transactions_data(num_records=num_records, output_file=test_csv_filename)

    # --- NOVA IMPLEMENTAÇÃO: Ingestão dos dados gerados no DB ---
    # O ingestor lê o arquivo que acabamos de gerar e o carrega no DB.
    ingest_data_to_db(file_path=os.path.join(RAW_DATA_PATH, test_csv_filename), table_name="transactions")

    # --- NOVA IMPLEMENTAÇÃO: Execução da validação sobre os dados ingeridos ---
    # O validador deve encontrar os erros injetados pelo gerador.
    run_data_validations(table_name="transactions")

    # --- NOVA IMPLEMENTAÇÃO: Verificação dos logs de validação ---
    with db_engine.connect() as connection:
        logs_df = pd.read_sql(text("SELECT validation_rule, COUNT(*) as count FROM data_validation_logs GROUP BY validation_rule;"), connection)
    
    # Converter para dicionário para fácil verificação
    logs_summary = logs_df.set_index('validation_rule')['count'].to_dict()

    # Baseado nas porcentagens do gerador, esperamos pelo menos um certo número de cada erro.
    # Usamos uma margem para flexibilidade, já que a injeção é probabilística.
    # Porcentagens do generator: Account ID (2%), Amount Nulo (2%), Amount Negativo (1%), Data Futura (1%), Duplicatas (0.5%)
    # Para 50 registros:
    # Account ID Nulo: ~1 (50 * 0.02)
    # Amount Nulo: ~1 (50 * 0.02)
    # Amount Negativo: ~0.5 (50 * 0.01) -> pode ser 0 ou 1
    # Data Futura: ~0.5 (50 * 0.01) -> pode ser 0 ou 1
    # Duplicatas de ID são tratadas na ingestão (ignorar dups, então não aparecerão como erro de validação de campo)
    
    # Verificamos se as regras de validação que esperam erros foram acionadas.
    # Como são 50 registros, a probabilidade de ter pelo menos 1 é alta, mas não garantida 100%.
    # Usaremos uma verificação para garantir que as regras EXISTEM nos logs, não necessariamente uma quantidade mínima.
    
    # É mais robusto checar se as regras esperadas ESTÃO presentes nos logs do que um número exato de erros,
    # dado o caráter probabilístico da injeção de erros no gerador para um volume pequeno de dados.
    assert "Account ID Nulo" in logs_summary, "Regra 'Account ID Nulo' não foi detectada nos logs."
    assert "Amount Nulo" in logs_summary, "Regra 'Amount Nulo' não foi detectada nos logs."
    assert "Amount Negativo" in logs_summary, "Regra 'Amount Negativo' não foi detectada nos logs."
    assert "Data Futura" in logs_summary, "Regra 'Data Futura' não foi detectada nos logs."
    # A regra de Categoria Inválida não é injetada aleatoriamente no gerador, então não esperamos vê-la aqui.
    
    # --- Verificação da ingestão: Quantidade de registros na tabela transactions ---
    with db_engine.connect() as connection:
        transactions_count = connection.execute(text("SELECT COUNT(*) FROM transactions;")).scalar()
    
    # O ingestor trata duplicatas de ID, então o número final pode ser ligeiramente menor que num_records.
    # No entanto, como o gerador injeta poucas duplicatas para 50 registros, esperamos algo próximo.
    # Vamos verificar se o número de transações é o esperado ou ligeiramente menor devido a dups.
    assert transactions_count <= num_records and transactions_count >= (num_records - 1), \
        f"Número de transações ingeridas ({transactions_count}) não corresponde ao esperado ({num_records} ou {num_records-1} se houver 1 duplicata)."
    
    logging.info(f"Relatório de logs de integração: {logs_summary}")
    logging.info(f"Total de transações ingeridas no DB: {transactions_count}")