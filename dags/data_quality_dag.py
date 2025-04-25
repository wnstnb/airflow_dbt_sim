from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os

DATA_DIR = "/opt/airflow/data"

def compare_raw_clean():
    """Compare raw and clean customer data and print statistics."""
    raw_db_uri = f"sqlite:///{DATA_DIR}/raw_data.db"
    clean_db_uri = f"sqlite:///{DATA_DIR}/main_main.db"

    # create engine
    engine = create_engine(raw_db_uri)
    clean_engine = create_engine(clean_db_uri)

    # Get raw data using the URI
    raw_df = pd.read_sql_query("SELECT * FROM customers_raw", con=engine)

    # Get clean data using the URI
    clean_df = pd.read_sql_query("SELECT * FROM customers_clean", con=clean_engine)
    
    # Generate comparison report
    comparison = {
        "Total rows in raw": len(raw_df),
        "Total rows in clean": len(clean_df),
        "Rows removed": len(raw_df) - len(clean_df),
        "Null credit scores in raw": raw_df['CreditScore'].isnull().sum(),
        "Null credit scores in clean": clean_df['CreditScore'].isnull().sum(),
    }
    
    # Print comparison
    print("\n=== Data Quality Report ===")
    for metric, value in comparison.items():
        print(f"{metric}: {value}")
    
    # Basic statistics comparison
    print("\n=== Credit Score Statistics ===")
    print("\nRaw Data Statistics:")
    print(raw_df['CreditScore'].describe())
    print("\nClean Data Statistics:")
    print(clean_df['CreditScore'].describe())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_quality_pipeline',
    default_args=default_args,
    description='Transform raw data to clean data using dbt',
    schedule=timedelta(days=1),
) as dag:

    # Run dbt to transform data from raw_data.db to clean_data.db
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/clean_the_data && dbt run',
    )

    # Compare data between raw and clean databases
    compare_data = PythonOperator(
        task_id='compare_data',
        python_callable=compare_raw_clean,
    )

    # Set task dependencies
    run_dbt >> compare_data 