# 🏪 Northwind Data Pipeline

Pipeline de dados completa para análise de performance comercial da Northwind Trading Co., construída com Python, dbt e PostgreSQL seguindo a arquitetura medallion (Bronze → Silver → Gold).

---

## 📐 Arquitetura

```
CSVs / Google Sheets
        │
        ▼
┌───────────────────┐
│   Bronze Layer    │  Python + pandas + SQLAlchemy
│  schema: bronze   │  Dado bruto, sem transformações
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   Silver Layer    │  dbt (views)
│  schema: silver   │  Limpeza, tipagem, campos calculados
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│    Gold Layer     │  dbt (tables)
│   schema: gold    │  KPIs, agregações, RFM, rankings
└────────┬──────────┘
         │
         ▼
  Looker Studio / Data Studio
```

---

## 📁 Estrutura do Projeto

```
northwind_project/
├── .venv/                        # Ambiente virtual (não versionar)
├── data/                         # 14 CSVs da Northwind
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
│   └── 01_ingest_bronze.py       # Ingestão Python → bronze
├── northwind_dbt/                # Projeto dbt
│   ├── dbt_project.yml
│   └── models/
│       ├── silver/
│       │   ├── schema.yml
│       │   ├── stg_orders.sql
│       │   ├── stg_order_details.sql
│       │   ├── stg_customers.sql
│       │   ├── stg_products.sql
│       │   ├── stg_employees.sql
│       │   └── stg_shippers.sql
│       └── gold/
│           ├── schema.yml
│           ├── fct_orders.sql
│           ├── kpi_revenue_monthly.sql
│           ├── kpi_customers_rfm.sql
│           ├── kpi_products_performance.sql
│           ├── kpi_employees_performance.sql
│           ├── kpi_geo_revenue.sql
│           └── kpi_logistics.sql
└── requirements.txt
```

---

## ✅ Pré-requisitos

Antes de começar, certifique-se de ter instalado:

- **Python 3.10+** → [python.org/downloads](https://www.python.org/downloads/)
  - Durante a instalação no Windows, marque ☑ **"Add python.exe to PATH"**
- **PostgreSQL 15+** → [postgresql.org/download](https://www.postgresql.org/download/)
  - Anote a senha definida para o usuário `postgres` durante a instalação
  - Após instalar, adicione `C:\Program Files\PostgreSQL\{versão}\bin` às variáveis de ambiente (Windows)

---

## 🚀 Passo a Passo

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/northwind-pipeline.git
cd northwind-pipeline
```

### 2. Crie e ative o ambiente virtual

```bash
# Criar
python -m venv .venv

# Ativar — Mac/Linux
source .venv/bin/activate

# Ativar — Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Ativar — Windows (cmd)
.venv\Scripts\activate.bat
```

> ⚠️ O terminal deve exibir `(.venv)` no início da linha. Sempre ative o ambiente virtual ao abrir um novo terminal.

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Configure o banco de dados PostgreSQL

Abra o terminal do PostgreSQL:

```bash
# Mac/Linux
psql -U postgres

# Windows
psql -U postgres -h localhost
```

Execute os comandos abaixo dentro do psql:

```sql
CREATE USER northwind_user WITH PASSWORD 'northwind123';
CREATE DATABASE northwind_db OWNER northwind_user;
GRANT ALL PRIVILEGES ON DATABASE northwind_db TO northwind_user;
\q
```

Teste a conexão:

```bash
psql -h localhost -U northwind_user -d northwind_db
# Senha: northwind123
# Se aparecer "northwind_db=>" está funcionando — digite \q para sair
```

### 5. Ajuste o caminho dos arquivos no script de ingestão

Abra `scripts/01_ingest_bronze.py` e atualize a variável `DATA_DIR` com o caminho absoluto da pasta `data/` no seu computador:

```python
# Linux/Mac
DATA_DIR = "/home/seu-usuario/northwind-pipeline/data"

# Windows
DATA_DIR = r"C:\Users\SeuNome\northwind-pipeline\data"
```

### 6. Execute a ingestão (camada Bronze)

```bash
python scripts/01_ingest_bronze.py
```

Saída esperada:

```
✓ bronze.orders               830 linhas
✓ bronze.order_details       2155 linhas
✓ bronze.customers             91 linhas
...
INGESTÃO COMPLETA — 3362 linhas carregadas em bronze.
```

### 7. Configure o perfil do dbt

Crie o arquivo `~/.dbt/profiles.yml` (fora do projeto, na pasta do seu usuário):

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

> 📁 No Windows o arquivo fica em `C:\Users\SeuNome\.dbt\profiles.yml`

### 8. Execute o dbt

Entre na pasta do projeto dbt:

```bash
cd northwind_dbt
```

Teste a conexão:

```bash
dbt debug
# Esperado: Connection test: [OK connection ok]
```

Execute as transformações:

```bash
dbt run
```

Saída esperada:

```
✓ silver.stg_customers         CREATE VIEW
✓ silver.stg_orders            CREATE VIEW
✓ silver.stg_products          CREATE VIEW
...
✓ gold.fct_orders              SELECT 2155
✓ gold.kpi_customers_rfm       SELECT 89
✓ gold.kpi_revenue_monthly     SELECT 23
...
Done. PASS=13 WARN=0 ERROR=0 SKIP=0 TOTAL=13
```

Execute os testes de qualidade:

```bash
dbt test
# Esperado: Done. PASS=33 WARN=0 ERROR=0 SKIP=0 TOTAL=33
```

---

## 🔄 Fluxo de Atualização dos Dados

Sempre que os arquivos CSV forem atualizados, execute na ordem:

```bash
# 1. Na raiz do projeto (com o venv ativo)
python scripts/01_ingest_bronze.py

# 2. Na pasta northwind_dbt
cd northwind_dbt
dbt run
dbt test
```

> ⚠️ O script de ingestão usa `DROP ... CASCADE`, o que remove as views silver e as tabelas gold antes de recriar o bronze. O `dbt run` seguinte as recria todas.

---

## 📊 Tabelas Disponíveis após o dbt run

| Schema | Tabela | Tipo | Linhas | Descrição |
|---|---|---|---|---|
| `bronze` | `orders`, `customers`, ... | TABLE | variado | Dado bruto dos CSVs |
| `public_silver` | `stg_orders` | VIEW | — | Pedidos limpos com status de entrega |
| `public_silver` | `stg_order_details` | VIEW | — | Itens com receita líquida calculada |
| `public_silver` | `stg_customers` | VIEW | — | Clientes com continente |
| `public_silver` | `stg_products` | VIEW | — | Produtos com status de estoque |
| `public_silver` | `stg_employees` | VIEW | — | Vendedores com tempo de casa |
| `public_silver` | `stg_shippers` | VIEW | — | Transportadoras |
| `public_gold` | `fct_orders` | TABLE | 2.155 | Fato central: pedido × produto |
| `public_gold` | `kpi_revenue_monthly` | TABLE | 23 | Receita mensal + variação MoM |
| `public_gold` | `kpi_customers_rfm` | TABLE | 89 | Segmentação RFM anti-churn |
| `public_gold` | `kpi_products_performance` | TABLE | 77 | Ranking ABC de produtos |
| `public_gold` | `kpi_employees_performance` | TABLE | 9 | Performance da equipe de vendas |
| `public_gold` | `kpi_geo_revenue` | TABLE | 21 | Receita por país e continente |
| `public_gold` | `kpi_logistics` | TABLE | 3 | SLA por transportadora |

---

## ☁️ Conectar no Looker Studio (opcional)

Para visualizar os dados no Looker Studio, o banco precisa estar acessível pela internet. Recomenda-se o **Supabase** (gratuito).

### Migração para o Supabase

1. Crie um projeto em [supabase.com](https://supabase.com)
2. Exporte o banco local:

```bash
pg_dump -h localhost -U northwind_user -d northwind_db -F p -f dump_northwind.sql
```

3. Importe no Supabase:

```bash
psql -h db.SEU-HOST.supabase.co -U postgres -d postgres -f dump_northwind.sql
```

4. Atualize o `profiles.yml` com as credenciais do Supabase e rode `dbt run` novamente.

### Conexão no Looker Studio

1. Acesse [lookerstudio.google.com](https://lookerstudio.google.com)
2. Criar → Fonte de dados → **PostgreSQL**
3. Preencha as credenciais do Supabase
4. Marque **"Ativar SSL"** e use a porta **6543**
5. Use **Consulta Personalizada** com `SELECT * FROM public_gold.fct_orders`

---

## 🧪 Testes dbt

O projeto inclui 33 testes de qualidade de dados distribuídos entre as camadas silver e gold:

| Tipo de teste | O que verifica |
|---|---|
| `unique` | Sem valores duplicados nas chaves primárias |
| `not_null` | Campos obrigatórios sempre preenchidos |
| `accepted_values` | Campos categóricos dentro dos valores esperados |

---

## 🛠️ Tecnologias

| Tecnologia | Versão | Função |
|---|---|---|
| Python | 3.10+ | Ingestão dos CSVs para o bronze |
| pandas | 2.x | Leitura e manipulação dos arquivos |
| SQLAlchemy | 2.x | Conexão com o PostgreSQL |
| PostgreSQL | 15+ | Banco de dados principal |
| dbt-core | 1.11 | Transformações silver e gold |
| dbt-postgres | 1.10 | Adapter PostgreSQL para o dbt |
| Supabase | — | Hospedagem do banco na nuvem |
| Looker Studio | — | Visualização e dashboards |

---

Este projeto foi desenvolvido para fins de teste com base no dataset público Northwind.
Todo o arquivo readme foi gerado a partir do Claude