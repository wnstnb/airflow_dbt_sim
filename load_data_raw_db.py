import subprocess
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime


DEFAULT_ARGS = {
    "owner": "you",
    "start_date": datetime(2025, 4, 23),
    "retries": 0,
}

DATA_DIR = r"C:\Projects\airflow_dbt_sim" # ← adjust this!

def load_csvs_to_raw(**context):
    """Read each CSV in data/ and write to raw_data.db."""
    raw_engine = create_engine(f"sqlite:///{DATA_DIR}/raw_data.db")
    tables = {
        "customers.csv": "customers_raw",
        "accounts.csv": "accounts_raw",
        "monthly_balances.csv": "monthly_balances_raw",
    }
    for csv_file, table_name in tables.items():
        df = pd.read_csv(os.path.join(DATA_DIR, "data", csv_file))
        df.to_sql(table_name, raw_engine, if_exists="replace", index=False)

load_csvs_to_raw()