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