import pandas as pd
from sqlalchemy import create_engine, text
import logging, sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

DB_URL   = "postgresql://northwind_user:northwind123@localhost:5432/northwind_db"
DATA_DIR = r"C:\Users\Felipe Bettecher\Env\northwind_project\data"

engine = create_engine(DB_URL)

with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
    conn.commit()
log.info("Schema 'bronze' criado/verificado.")

TABLES = {
    "categories":           {"file": "categories.csv",            "date_cols": []},
    "customers":            {"file": "customers.csv",             "date_cols": []},
    "employees":            {"file": "employees.csv",             "date_cols": ["birth_date", "hire_date"]},
    "employee_territories": {"file": "employee_territories.csv",  "date_cols": []},
    "order_details":        {"file": "order_details.csv",         "date_cols": []},
    "orders":               {"file": "orders.csv",                "date_cols": ["order_date", "required_date", "shipped_date"]},
    "products":             {"file": "products.csv",              "date_cols": []},
    "shippers":             {"file": "shippers.csv",              "date_cols": []},
    "suppliers":            {"file": "suppliers.csv",             "date_cols": []},
    "territories":          {"file": "territories.csv",           "date_cols": []},
    "region":               {"file": "region.csv",                "date_cols": []},
    "us_states":            {"file": "us_states.csv",             "date_cols": []},
}

total_rows = 0
for table_name, cfg in TABLES.items():
    filepath = f"{DATA_DIR}\\{cfg['file']}"
    try:
        df = pd.read_csv(
            filepath, sep=";", on_bad_lines="skip",
            parse_dates=cfg["date_cols"] if cfg["date_cols"] else False
        )
        df["_ingested_at"]  = pd.Timestamp.now()
        df["_source_file"]  = cfg["file"]

        df.to_sql(
            table_name, engine,
            schema="bronze",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=500,
        )
        log.info(f"  ✓ bronze.{table_name:<30} {len(df):>5} linhas")
        total_rows += len(df)
    except Exception as e:
        log.error(f"  ✗ {table_name}: {e}")

log.info(f"INGESTÃO COMPLETA — {total_rows} linhas carregadas em bronze.")