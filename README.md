# Plataforma de Qualidade de Dados para Transações Financeiras

-----

## Visão Geral do Projeto

Este projeto implementa um pipeline de dados para simular a ingestão e validação de transações financeiras. O objetivo principal é demonstrar as melhores práticas em Engenharia de Dados, incluindo:

  * **Geração de Dados Sintéticos:** Criação de um conjunto de dados realista com erros intencionais para simular cenários de qualidade de dados.
  * **Ingestão de Dados:** Carregamento eficiente dos dados gerados para um banco de dados PostgreSQL.
  * **Validação de Dados:** Aplicação de regras de negócio e integridade para identificar e logar anomalias nos dados.
  * **Testes Automatizados:** Implementação de testes unitários e de integração para garantir a confiabilidade e robustez do pipeline.

-----

## Arquitetura

O projeto é modularizado, com cada componente desempenhando uma função específica:

  * `src/data_generation/generator.py`: Script para gerar dados transacionais sintéticos em formato CSV.
  * `src/ingestion/ingestor.py`: Módulo responsável por ler os arquivos CSV e ingeri-los no banco de dados.
  * `src/validation/validator.py`: Contém as regras de validação de dados e as aplica aos dados ingeridos, registrando as violações.
  * `src/utils/db_utils.py`: Funções utilitárias para conexão e interação com o banco de dados.
  * `config.py`: Armazena configurações do ambiente (variáveis de ambiente, caminhos de arquivos, etc.).
  * `main.py`: Orquestra a execução de todo o pipeline.
  * `tests/`: Contém os testes unitários e de integração do projeto.

\<br\>

**Fluxo do Pipeline:**

`data_generation` (CSV) ➡️ `ingestion` (PostgreSQL) ➡️ `validation` (PostgreSQL - Logs)

-----

## Configuração do Ambiente

Siga os passos abaixo para configurar e executar o projeto em sua máquina local.

### Pré-requisitos

  * **Python 3.9+**
  * **Docker e Docker Compose** (para rodar o PostgreSQL localmente)

### 1\. Clonar o Repositório

```bash
git https://github.com/GSFerr/plataforma-qualidade-dados.git
cd plataforma-qualidade-dados
```

### 2\. Configurar o Banco de Dados PostgreSQL

Este projeto utiliza Docker Compose para levantar um serviço de banco de dados PostgreSQL.

Primeiro, crie um arquivo chamado `.env` na raiz do seu projeto (se ele ainda não existir) e adicione o seguinte conteúdo. Você pode ajustar os valores conforme sua necessidade:

```dotenv
# .env
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=plataforma_qualidade_dados
```

Em seguida, inicie o contêiner do PostgreSQL. Certifique-se de que o Docker esteja em execução:

```bash
docker-compose up -d
```

Após o PostgreSQL iniciar, crie as tabelas necessárias no banco de dados executando o utilitário de banco de dados:

```bash
python src/utils/db_utils.py
```

### 3\. Criar e Ativar Ambiente Virtual

É altamente recomendado usar um ambiente virtual para gerenciar as dependências do projeto.

```bash
python -m venv .venv
# No Windows (PowerShell):
.\.venv\Scripts\activate
# No macOS/Linux (Bash/Zsh):
source .venv/bin/activate
```

### 4\. Instalar Dependências

Com o ambiente virtual ativado, instale todas as bibliotecas Python necessárias listadas no `requirements.txt`:

```bash
pip install -r requirements.txt
```

-----

## Estrutura do Projeto

```
plataforma-qualidade-dados/
├── .env                  # Variáveis de ambiente para configurações do DB
├── .gitignore            # Arquivos e diretórios a serem ignorados pelo Git
├── docker-compose.yml    # Configuração do Docker para o serviço PostgreSQL
├── main.py               # Ponto de entrada principal do pipeline
├── config.py             # Configurações gerais do projeto (caminhos, nome de arquivo padrão, etc.)
├── requirements.txt      # Dependências Python do projeto
├── README.md             # Este arquivo de documentação
├── src/                  # Código-fonte principal da aplicação
│   ├── data_generation/  # Geração de dados sintéticos para o pipeline
│   │   └── generator.py
│   ├── ingestion/        # Módulo responsável pela ingestão de dados no DB
│   │   └── ingestor.py
│   ├── validation/       # Regras de validação e log de violações
│   │   └── validator.py
│   └── utils/            # Funções utilitárias (e.g., conexão com DB, criação de tabelas)
│       └── db_utils.py
└── tests/                # Testes automatizados
    ├── unit/             # Testes unitários para módulos individuais
    │   ├── test_generator.py
    │   └── test_validator.py
    └── integration/      # Testes de integração para o fluxo completo do pipeline
        └── test_pipeline_integration.py
```

-----

## Como Executar o Pipeline

Após configurar o ambiente (seguindo os passos em "Configuração do Ambiente" acima), você pode executar o pipeline completo:

```bash
# Certifique-se de que seu ambiente virtual está ativado
# No Windows (PowerShell):
# .\.venv\Scripts\activate
# No macOS/Linux (Bash/Zsh):
# source .venv/bin/activate

python main.py
```

Este comando irá:

1.  Gerar um novo arquivo `transactions.csv` (ou o nome definido em `config.py`) na pasta `data/raw`.
2.  Ingerir os dados deste CSV na tabela `transactions` do PostgreSQL (previamente truncada para garantir um estado limpo).
3.  Executar as validações nos dados recém-ingeridos, registrando quaisquer violações de qualidade na tabela `data_validation_logs`.

-----

## Executando os Testes

Para garantir a qualidade e o correto funcionamento do pipeline, execute os testes automatizados:

```bash
# Certifique-se de que seu ambiente virtual está ativado
# No Windows (PowerShell):
# .\.venv\Scripts\activate
# No macOS/Linux (Bash/Zsh):
# source .venv/bin/activate

# Para rodar todos os testes (unitários e de integração):
pytest

# Para rodar apenas os testes unitários:
pytest tests/unit/

# Para rodar apenas os testes de integração:
pytest tests/integration/
```
