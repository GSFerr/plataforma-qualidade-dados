# src/utils/db_utils.py
import logging
import urllib.parse # Importar o módulo urllib.parse para escapar a senha
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

# Configuração de logging para boas práticas
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_engine():
    """
    Cria e retorna um objeto SQLAlchemy Engine para conexão com o PostgreSQL.
    Utiliza variáveis de ambiente para as credenciais, seguindo boas práticas de segurança.

    Returns:
        sqlalchemy.engine.base.Engine: Objeto Engine do SQLAlchemy.
    Raises:
        SQLAlchemyError: Se houver um erro na criação do engine ou conexão inicial.
    """
    try:
        # **NOVA LINHA:** Escapa a senha para lidar com caracteres especiais como '@', ':' etc.
        # Verifica se DB_PASSWORD não é None antes de tentar escapar
        escaped_password = urllib.parse.quote_plus(DB_PASSWORD) if DB_PASSWORD else ""

        # Formato da string de conexão para PostgreSQL com psycopg2
        # Usa a senha escapada na string de conexão
        db_connection_str = (
            f"postgresql+psycopg2://{DB_USER}:{escaped_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        engine = create_engine(db_connection_str, pool_pre_ping=True)
        logging.info("SQLAlchemy Engine criado com sucesso.")
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Erro ao criar o SQLAlchemy Engine: {e}")
        # Para depuração, podemos imprimir a string de conexão (CUIDADO em produção!)
        # logging.error(f"String de conexão tentada: postgresql+psycopg2://{DB_USER}:*****@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        raise # Re-lança a exceção para que o chamador possa tratá-la

def get_db_session():
    """
    Cria e retorna uma sessão do SQLAlchemy.
    Recomendado para operações de banco de dados que precisam de transações.

    Returns:
        sqlalchemy.orm.session.Session: Objeto Session do SQLAlchemy.
    Raises:
        SQLAlchemyError: Se houver um erro na criação do engine ou sessão.
    """
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logging.info("SQLAlchemy Session criada com sucesso.")
    return session

def test_db_connection():
    """
    Testa a conexão com o banco de dados.
    """
    logging.info("Testando conexão com o banco de dados...")
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            logging.info("Conexão com o banco de dados estabelecida e testada com sucesso!")
        return True
    except SQLAlchemyError as e:
        logging.error(f"Falha ao conectar ao banco de dados: {e}")
        return False
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado ao testar a conexão: {e}")
        return False

if __name__ == "__main__":
    test_db_connection()