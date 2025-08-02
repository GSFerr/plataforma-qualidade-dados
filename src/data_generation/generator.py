# src/data_generation/generator.py
import pandas as pd
import numpy as np
import uuid
import random
from datetime import datetime, timedelta
import logging
import os
from config import RAW_DATA_PATH # Importa o caminho da pasta de dados brutos

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_transactions_data(num_records: int = 1000, output_file: str = "transactions.csv"):
    """
    Gera um DataFrame de dados de transações financeiras simuladas
    e o salva em um arquivo CSV na pasta RAW_DATA_PATH.
    Inclui intencionalmente erros para teste de validação.

    Args:
        num_records (int): Número de registros de transações a serem gerados.
        output_file (str): Nome do arquivo CSV de saída.
    """
    logging.info(f"Iniciando a geração de {num_records} registros de transações simuladas...")

    # Listas de dados para simulação
    transaction_types = ['DEBITO', 'CREDITO', 'PIX', 'TRANSFERENCIA', 'PAGAMENTO_CONTA']
    currencies = ['BRL', 'USD', 'EUR']
    merchants = [
        'Supermercado XYZ', 'Loja de Roupas ABC', 'Restaurante Sabor',
        'Posto Gasolina', 'Farmácia Bem-Estar', 'Padaria Doce', 'App de Entrega'
    ]
    categories = ['ALIMENTACAO', 'COMPRAS', 'SERVICOS', 'TRANSPORTES', 'SAUDE', 'LAZER']
    statuses = ['CONCLUIDA', 'PENDENTE', 'FALHA']

    data = []
    for _ in range(num_records):
        transaction_id = str(uuid.uuid4())
        account_id = f"ACC{random.randint(100000, 999999)}"

        # Geração de datas e horas aleatórias nos últimos 30 dias
        date = (datetime.now() - timedelta(days=random.randint(0, 30))).date()
        time = (datetime.min + timedelta(seconds=random.randint(0, 86399))).time() # 86399 segundos em um dia

        amount = round(random.uniform(5.00, 5000.00), 2)
        currency = random.choice(currencies)
        transaction_type = random.choice(transaction_types)
        merchant_name = random.choice(merchants)
        category = random.choice(categories)
        status = random.choice(statuses)

        data.append([
            transaction_id, account_id, date, time, amount, currency,
            transaction_type, merchant_name, category, status
        ])

    df = pd.DataFrame(data, columns=[
        'transaction_id', 'account_id', 'transaction_date', 'transaction_time',
        'amount', 'currency', 'transaction_type', 'merchant_name',
        'category', 'status'
    ])

    # --- Inserindo ERROS INTENCIONAIS para testes de validação ---
    logging.info("Injetando erros intencionais nos dados para teste de validação...")

    # Erro 1: Valores nulos em campos obrigatórios (ex: amount, account_id)
    num_nulls = int(num_records * 0.02) # 2% de nulos
    df.loc[df.sample(n=num_nulls).index, 'amount'] = np.nan
    df.loc[df.sample(n=num_nulls).index, 'account_id'] = np.nan

    # Erro 2: Valores negativos em 'amount'
    num_negative_amounts = int(num_records * 0.01) # 1% de valores negativos
    df.loc[df.sample(n=num_negative_amounts).index, 'amount'] = -random.uniform(1.00, 100.00)

    # Erro 3: Datas inválidas (futuras)
    num_future_dates = int(num_records * 0.01) # 1% de datas futuras
    for idx in df.sample(n=num_future_dates).index:
        df.loc[idx, 'transaction_date'] = (datetime.now() + timedelta(days=random.randint(1, 30))).date()

    # Erro 4: IDs de transação duplicados
    num_duplicates = int(num_records * 0.005) # 0.5% de duplicatas
    if num_duplicates > 0:
        duplicate_ids = df['transaction_id'].sample(n=num_duplicates, replace=True).tolist()
        # Adiciona novas linhas com IDs duplicados para garantir
        duplicate_rows = df[df['transaction_id'].isin(duplicate_ids)].copy()
        # Mude a data/hora ligeiramente para que não sejam exatamente as mesmas linhas duplicadas
        duplicate_rows['transaction_date'] = duplicate_rows['transaction_date'].apply(lambda d: d - timedelta(days=1))
        df = pd.concat([df, duplicate_rows], ignore_index=True)


    # --- Salvando os dados ---
    output_full_path = os.path.join(RAW_DATA_PATH, output_file)
    # Garante que o diretório exista
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    df.to_csv(output_full_path, index=False)
    logging.info(f"Dados gerados e salvos em: {output_full_path}. Total de registros (após duplicação): {len(df)}")

if __name__ == "__main__":
    # Exemplo de uso ao executar o módulo diretamente
    generate_transactions_data(num_records=5000) # Gerar 5000 registros para um bom volume