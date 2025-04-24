from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from faker import Faker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import random
import pandas as pd
import os

DEFAULT_ARGS = {
    "owner": "you",
    "start_date": datetime(2025, 4, 23),
    "retries": 0,
}

def generate_raw(**context):
    """Populate raw_data.db with a messy users_raw table."""
    engine = create_engine("sqlite:///raw_data.db")
    meta = MetaData()

    users = Table(
        "users_raw", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("full_name", String),
        Column("email", String),
        Column("age", Integer),
    )
    meta.drop_all(engine, tables=[users])
    meta.create_all(engine, tables=[users])

    fake = Faker()
    conn = engine.connect()
    for _ in range(100):
        name = fake.name() if random.random() > 0.1 else None
        email = fake.email() if random.random() > 0.2 else "invalid-email"
        age = random.choice([fake.random_int(18, 90), -1, None])
        conn.execute(users.insert().values(full_name=name, email=email, age=age))
    conn.close()

def copy_to_processed(**context):
    """Read clean_users from raw_data.db and write to processed_data.db."""
    raw_engine = create_engine("sqlite:///raw_data.db")
    df = pd.read_sql("SELECT * FROM clean_users", raw_engine)

    processed_engine = create_engine("sqlite:///processed_data.db")
    df.to_sql("users_clean", processed_engine, if_exists="replace", index=False)

    # Optional sanity-check
    with processed_engine.connect() as conn:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        print("Processed DB tables:", [t[0] for t in tables])

with DAG(
    "sqlite_full_circle_etl",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    t1_generate = PythonOperator(
        task_id="generate_raw_data",
        python_callable=generate_raw,
    )

    t2_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /path/to/dbt_project && "
            "dbt run --profiles-dir ."
        ),
    )

    t3_copy = PythonOperator(
        task_id="copy_to_processed_db",
        python_callable=copy_to_processed,
    )

    t1_generate >> t2_dbt >> t3_copy
