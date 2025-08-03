# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configurações de Banco de Dados (PostgreSQL) ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "plataforma_qualidade_dados")
DB_USER = os.getenv("DB_USER", "postgres")
# **A SENHA DEVE VIR SOMENTE DO ARQUIVO .ENV**
# Remova o valor padrão 'Pos@2025app' daqui.
DB_PASSWORD = os.getenv("DB_PASSWORD") # Apenas tenta ler a variável de ambiente. Se não encontrar, será None.
DB_PORT = os.getenv("DB_PORT", "5432")

# --- Configurações AWS (para futuras etapas, se aplicável) ---
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# --- Caminhos de Arquivos e Pastas ---
RAW_DATA_PATH = "data/raw/"
PROCESSED_DATA_PATH = "data/processed/"

# --- NOVA IMPLEMENTAÇÃO / ALTERAÇÃO DE CÓDIGO EXISTENTE ---
# Definir o nome do arquivo de dados brutos padrão
RAW_DATA_FILE = "transactions.csv" 
# Certifique-se de que este nome corresponda ao arquivo gerado pelo seu generator.py
# e que é o nome padrão que o ingestor deve procurar.

# Configurações de Logging (se houver, manter)
# Exemplo: LOG_FILE_PATH = os.path.join(BASE_DIR, 'logs', 'app.log')