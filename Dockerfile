FROM apache/airflow:2.7.3

USER root
# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Install Python packages including dbt-core and sqlite adapter
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-sqlite

# Set working directory
WORKDIR /opt/airflow/dags 