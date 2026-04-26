# Northwind Data Pipeline

Pipeline de dados para análise de performance comercial da Northwind Trading Co., construída com Python, dbt e PostgreSQL seguindo a arquitetura medallion (Bronze → Silver → Gold).

---

## Arquitetura

```
CSVs
  │
  ▼
Bronze Layer  →  Python + pandas + SQLAlchemy  →  schema: bronze  (dado bruto)
  │
  ▼
Silver Layer  →  dbt (views)                  →  schema: silver  (limpeza e tipagem)
  │
  ▼
Gold Layer    →  dbt (tables)                 →  schema: gold    (KPIs e agregações)
```

---

## Estrutura do Projeto

```
northwind_project/
├── data/                         # CSVs da Northwind
│   ├── categories.csv
│   ├── customers.csv
│   ├── employees.csv
│   ├── employee_territories.csv
│   ├── order_details.csv
│   ├── orders.csv
│   ├── products.csv
│   ├── shippers.csv
│   ├── suppliers.csv
│   ├── territories.csv
│   ├── region.csv
│   └── us_states.csv
├── scripts/
│   └── ingestion.py              # Ingestão Python → bronze
├── northwind_dbt/                # Projeto dbt
│   ├── dbt_project.yml
│   └── models/
│       ├── silver/
│       │   ├── schema.yml
│       │   ├── stg_customers.sql
│       │   ├── stg_employees.sql
│       │   ├── stg_order_details.sql
│       │   ├── stg_orders.sql
│       │   ├── stg_products.sql
│       │   └── stg_shippers.sql
│       └── gold/
│           ├── schema.yml
│           ├── fct_orders.sql
│           └── customers_rfm.sql
├── requirements.txt
└── readme.md
```

---

## Pré-requisitos

- **Python 3.10+** — marque "Add python.exe to PATH" durante a instalação no Windows
- **PostgreSQL 15+** — anote a senha do usuário `postgres` e adicione o diretório `bin` às variáveis de ambiente

---

## Passo a Passo

### 1. Clone o repositório

```bash
git clone https://github.com/fsbettecher/northwind-pipeline.git
cd northwind_project
```

### 2. Crie e ative o ambiente virtual

```bash
python -m venv .venv

# Mac/Linux
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Windows (cmd)
.venv\Scripts\activate.bat
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Configure o banco de dados PostgreSQL

```bash
psql -U postgres -h localhost
```

```sql
CREATE USER northwind_user WITH PASSWORD 'northwind123';
CREATE DATABASE northwind_db OWNER northwind_user;
GRANT ALL PRIVILEGES ON DATABASE northwind_db TO northwind_user;
\q
```

### 5. Ajuste o caminho dos dados no script de ingestão

Abra `scripts/ingestion.py` e atualize a variável `DATA_DIR` com o caminho absoluto da pasta `data/`:

```python
# Windows
DATA_DIR = r"C:\Users\SeuNome\northwind_project\data"

# Mac/Linux
DATA_DIR = "/home/seu-usuario/northwind_project/data"
```

### 6. Execute a ingestão (camada Bronze)

```bash
python scripts/ingestion.py
```

### 7. Configure o perfil do dbt

Crie o arquivo `~/.dbt/profiles.yml` (no Windows: `C:\Users\SeuNome\.dbt\profiles.yml`):

```yaml
northwind_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: northwind_user
      password: northwind123
      dbname: northwind_db
      schema: public
      threads: 4
```

### 8. Execute o dbt

```bash
cd northwind_dbt

dbt debug        # verifica a conexão
dbt run          # executa as transformações
dbt test         # valida a qualidade dos dados
```

---

## Modelos Disponíveis

| Schema | Modelo | Tipo | Descrição |
|---|---|---|---|
| `silver` | `stg_orders` | VIEW | Pedidos com status de entrega e dias de atraso |
| `silver` | `stg_order_details` | VIEW | Itens com receita bruta, líquida e tier de desconto |
| `silver` | `stg_customers` | VIEW | Clientes com tratamento de região nula |
| `silver` | `stg_products` | VIEW | Produtos com status de estoque e categoria |
| `silver` | `stg_employees` | VIEW | Colaboradores com nome completo e tempo de casa |
| `silver` | `stg_shippers` | VIEW | Transportadoras |
| `gold` | `fct_orders` | TABLE | Fato central: pedido × produto (2.155 linhas) |
| `gold` | `customers_rfm` | TABLE | Segmentação RFM por cliente (89 linhas) |

---

## Testes de Qualidade

O projeto inclui 33 testes distribuídos entre as camadas silver e gold:

| Tipo | O que verifica |
|---|---|
| `unique` | Chaves primárias sem duplicatas |
| `not_null` | Campos obrigatórios sempre preenchidos |
| `accepted_values` | Campos categóricos dentro dos valores esperados |

---

## Fluxo de Atualização

Sempre que os CSVs forem atualizados, execute na ordem:

```bash
# Na raiz do projeto
python scripts/ingestion.py

# Na pasta northwind_dbt
cd northwind_dbt
dbt run
dbt test
```

> O script de ingestão usa `DROP ... CASCADE`, removendo as views silver e as tabelas gold antes de recriar o bronze. O `dbt run` as recria em seguida.

---

## Tecnologias

| Tecnologia | Versão | Função |
|---|---|---|
| Python | 3.10+ | Ingestão dos CSVs para o bronze |
| pandas | 3.x | Leitura e manipulação dos arquivos |
| SQLAlchemy | 2.x | Conexão com o PostgreSQL |
| PostgreSQL | 15+ | Banco de dados principal |
| dbt-core | 1.11 | Transformações silver e gold |
| dbt-postgres | 1.10 | Adapter PostgreSQL para o dbt |

---

Projeto desenvolvido para fins de estudo com base no dataset público Northwind.
