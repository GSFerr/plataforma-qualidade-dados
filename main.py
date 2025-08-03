# main.py
import logging
from src.utils.db_utils import test_db_connection
from src.data_generation.generator import generate_transactions_data
from src.ingestion.ingestor import ingest_data_to_db
from src.validation.validator import run_data_validations

# Configuração de logging para o ponto de entrada principal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_application():
    """
    Ponto de entrada principal da aplicação.
    Orquestra as diferentes etapas do pipeline de qualidade de dados.
    """
    logging.info("Iniciando a plataforma de qualidade de dados e monitoramento...")

    if not test_db_connection():
        logging.error("Não foi possível conectar ao banco de dados. Encerrando a aplicação.")
        return

    logging.info("Conexão com o banco de dados estabelecida. Prosseguindo com as operações...")

    # --- CHAMADA PARA A GERAÇÃO DE DADOS ---
    logging.info("Gerando dados de transações simuladas...")
    generate_transactions_data(num_records=10000) # Vamos gerar 10 mil para ter um volume bom

    # 2. Ingestão de Dados (NOVO PASSO)
    logging.info("Iniciando a ingestão de dados para o banco de dados...")
    ingest_data_to_db() # Chama a função de ingestão

    # --- 3. Validação de Dados ---
    logging.info("Iniciando a validação de dados...")
    run_data_validations() # Chama a função de validação

    # --- FUTURAS ETAPAS DO PIPELINE SERÃO CHAMADAS AQUI ---
    # from src.ingestion.ingestor import ingest_data
    # logging.info("Ingerindo dados de transações no banco de dados...")
    # ingest_data()

    logging.info("Plataforma de qualidade de dados concluída com sucesso (fase inicial de geração de dados).")

if __name__ == "__main__":
    run_application()