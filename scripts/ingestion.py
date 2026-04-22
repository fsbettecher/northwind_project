# Importando as bibliotecas
import pandas as pd
from sqlalchemy import create_engine, text

# Configurações de conexão e diretório dos arquivos
url   = "postgresql://northwind_user:northwind123@localhost:5432/northwind_db"
diretorio = r"C:\Users\Felipe Bettecher\Env\northwind_project\data"

engine = create_engine(url)
conn = engine.connect()

# Criando o schema bronze caso não exista
sql = """
CREATE SCHEMA IF NOT EXISTS bronze;
"""
conn.execute(text(sql))
conn.commit()

# Definindo a lista de tabelas e seus arquivos de origem
table_list = {
    "categories": "categories.csv",
    "customers": "customers.csv",
    "employees": "employees.csv",
    "employee_territories": "employee_territories.csv",
    "order_details": "order_details.csv",
    "orders": "orders.csv",
    "products": "products.csv",
    "shippers": "shippers.csv",
    "suppliers": "suppliers.csv",
    "territories": "territories.csv",
    "region": "region.csv",
    "us_states": "us_states.csv",
}

# Lendo os arquivos e carregando os dados no banco
for value in table_list:
    filepath = f"{diretorio}\\{table_list[value]}"

    try:
        df = pd.read_csv(filepath, sep=";")

        # Definindo hora da ingestão e nome do arquivo fonte
        df["ingested_at"] = pd.Timestamp.now()
        df["source_file"] = table_list[value]

        # Removendo a tabela caso já exista
        sql = f"""
        DROP TABLE IF EXISTS bronze.{value} CASCADE;
        """
        conn.execute(text(sql))
        conn.commit()

        # Enviando dados para o banco
        df.to_sql(
            value, engine,
            schema="bronze",
            if_exists="replace",
            index=False,
            method="multi"
        )
    except Exception as e:
        print(f"{value}: {e}")
