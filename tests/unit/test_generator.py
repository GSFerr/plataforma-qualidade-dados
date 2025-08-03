# tests/unit/test_generator.py
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import pytest
from src.data_generation.generator import generate_transactions_data
from config import RAW_DATA_PATH # Importa o caminho da pasta de dados brutos para limpeza

# --- FUNÇÃO AUXILIAR PARA LIMPEZA PÓS-TESTE ---
# Isso garante que os arquivos CSV gerados durante os testes sejam removidos.
@pytest.fixture(scope="module")
def setup_teardown_csv():
    """Fixture para garantir que o diretório de dados brutos esteja limpo antes e depois dos testes."""
    # Garante que o diretório RAW_DATA_PATH exista
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    
    # Limpeza antes do teste, caso haja algum resquício
    test_file_path = os.path.join(RAW_DATA_PATH, "test_transactions.csv")
    if os.path.exists(test_file_path):
        os.remove(test_file_path)
    
    yield # O teste será executado aqui
    
    # Limpeza após o teste
    if os.path.exists(test_file_path):
        os.remove(test_file_path)
    # Opcional: remover o diretório se estiver vazio (cuidado para não remover outros arquivos!)
    # if not os.listdir(RAW_DATA_PATH):
    #     os.rmdir(RAW_DATA_PATH)

def test_generate_transactions_data_structure(setup_teardown_csv):
    """
    Testa se o gerador cria um arquivo CSV com a estrutura de colunas esperada.
    """
    output_file = "test_transactions.csv"
    num_records = 10
    generate_transactions_data(num_records=num_records, output_file=output_file)

    df = pd.read_csv(os.path.join(RAW_DATA_PATH, output_file))

    expected_columns = [
        'transaction_id', 'account_id', 'transaction_date', 'transaction_time',
        'amount', 'currency', 'transaction_type', 'merchant_name',
        'category', 'status'
    ]
    assert list(df.columns) == expected_columns, "As colunas do DataFrame não correspondem às esperadas."
    assert len(df) >= num_records, "O número de registros gerados é menor que o esperado (pode ter duplicação intencional)."
    assert not df.empty, "O DataFrame gerado está vazio."

def test_generate_transactions_data_intentional_errors(setup_teardown_csv):
    """
    Testa se os erros intencionais (nulos, negativos, datas futuras) são injetados.
    Como a injeção é percentual, esperamos que pelo menos um erro de cada tipo seja encontrado.
    """
    output_file = "test_transactions_errors.csv"
    num_records = 1000 # Um número maior para aumentar a probabilidade de erros
    generate_transactions_data(num_records=num_records, output_file=output_file)

    df = pd.read_csv(os.path.join(RAW_DATA_PATH, output_file))

    # Teste para Nulos em 'amount'
    assert df['amount'].isnull().sum() > 0, "Nenhum valor nulo em 'amount' foi injetado."
    # Teste para Nulos em 'account_id'
    assert df['account_id'].isnull().sum() > 0, "Nenhum valor nulo em 'account_id' foi injetado."

    # Teste para Valores Negativos em 'amount'
    assert (df['amount'] < 0).sum() > 0, "Nenhum valor negativo em 'amount' foi injetado."

    # Teste para Datas Futuras em 'transaction_date'
    # Converte para datetime para comparação correta
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    today = datetime.now().date()
    assert (df['transaction_date'].dt.date > today).sum() > 0, "Nenhuma data futura em 'transaction_date' foi injetada."

    # Teste para Duplicatas em 'transaction_id'
    # Contamos o número de IDs únicos e comparamos com o total de linhas.
    # Como adicionamos duplicatas, esperamos que haja menos IDs únicos que o total de linhas.
    assert df['transaction_id'].duplicated().any(), "Nenhum transaction_id duplicado foi injetado."
    assert df['transaction_id'].nunique() < len(df), "O número de IDs únicos deveria ser menor que o total de registros devido a duplicatas."

def test_generate_transactions_data_num_records(setup_teardown_csv):
    """
    Testa se o gerador cria um número aproximado de registros (considerando duplicação).
    """
    output_file = "test_transactions_count.csv"
    num_records_expected = 100
    generate_transactions_data(num_records=num_records_expected, output_file=output_file)

    df = pd.read_csv(os.path.join(RAW_DATA_PATH, output_file))
    
    # O gerador adiciona 0.5% de duplicatas, então esperamos um pouco mais que num_records_expected
    # Um intervalo aceitável para o teste, dado que a duplicação é um percentual
    expected_min = num_records_expected
    expected_max = num_records_expected + int(num_records_expected * 0.005) + 5 # +5 para margem de segurança
    
    assert len(df) >= expected_min and len(df) <= expected_max, \
        f"Número de registros gerados ({len(df)}) fora da faixa esperada [{expected_min}, {expected_max}]."

def test_generator_file_creation(setup_teardown_csv):
    """
    Testa se o arquivo CSV é realmente criado no caminho esperado.
    """
    output_file = "another_test_file.csv"
    output_full_path = os.path.join(RAW_DATA_PATH, output_file)
    
    # Garante que o arquivo não exista antes de gerar
    if os.path.exists(output_full_path):
        os.remove(output_full_path)

    generate_transactions_data(num_records=5, output_file=output_file)
    
    assert os.path.exists(output_full_path), "O arquivo CSV não foi criado no caminho especificado."