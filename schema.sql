-- schema.sql

-- Criação do banco de dados para o projeto de qualidade de dados.
-- O "IF NOT EXISTS" evita erros caso o banco de dados já exista.
CREATE DATABASE plataforma_qualidade_dados
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Portuguese_Brazil.1252'
    LC_CTYPE = 'Portuguese_Brazil.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Conecta-se ao novo banco de dados para criar as tabelas dentro dele.
-- (Este comando é específico para ser executado em um cliente SQL como psql ou pgAdmin,
-- não diretamente no Python. No Python, a conexão já será para este DB.)
\c plataforma_qualidade_dados;

-- Tabela para armazenar as transações financeiras.
-- Demonstra a estrutura de dados brutos ou semi-processados.
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY, -- Identificador único da transação
    account_id VARCHAR(50) NOT NULL,       -- ID da conta do cliente
    transaction_date DATE NOT NULL,        -- Data da transação
    transaction_time TIME NOT NULL,        -- Hora da transação
    amount DECIMAL(18, 2) NOT NULL,        -- Valor da transação (permite 2 casas decimais)
    currency VARCHAR(3) NOT NULL,          -- Moeda da transação (ex: BRL, USD)
    transaction_type VARCHAR(50),          -- Tipo de transação (ex: DEBITO, CREDITO, PIX)
    merchant_name VARCHAR(100),            -- Nome do comerciante/destinatário
    category VARCHAR(50),                  -- Categoria da transação (ex: ALIMENTACAO, SERVICOS)
    status VARCHAR(20)                     -- Status da transação (ex: CONCLUIDA, PENDENTE, FALHA)
);

-- Tabela para armazenar os logs de validação de dados.
-- Essencial para monitorar a qualidade e identificar anomalias.
CREATE TABLE IF NOT EXISTS data_validation_logs (
    log_id SERIAL PRIMARY KEY,             -- ID único do log
    transaction_id VARCHAR(50),            -- ID da transação que gerou o erro (pode ser NULL se o erro for geral)
    validation_rule VARCHAR(100) NOT NULL, -- Nome da regra de validação que falhou
    error_message TEXT NOTOTE NULL,        -- Descrição do erro
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Quando o log foi registrado
);

-- Índices para otimização de consultas (boas práticas de BD)
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions (account_id);
CREATE INDEX IF NOT EXISTS idx_validation_logs_transaction_id ON data_validation_logs (transaction_id);
CREATE INDEX IF NOT EXISTS idx_validation_logs_rule ON data_validation_logs (validation_rule);