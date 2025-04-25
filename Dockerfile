FROM python:3.12-slim

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        sqlite3 \
        git \
        build-essential \
        default-libmysqlclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
ENV AIRFLOW_HOME=/opt/airflow

# Set Airflow configuration for initial user
ENV AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080
ENV AIRFLOW__WEBSERVER__SECRET_KEY=fd1390970b366d1766d085b808e4b37c
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True

# Set the admin user details
ENV AIRFLOW_USERNAME=admin
ENV AIRFLOW_PASSWORD=admin
ENV AIRFLOW_EMAIL=admin@example.com
ENV AIRFLOW_FIRSTNAME=admin
ENV AIRFLOW_LASTNAME=admin

# Install Python packages AS ROOT
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create airflow user
RUN useradd -ms /bin/bash airflow
RUN mkdir -p ${AIRFLOW_HOME} && chown -R airflow: ${AIRFLOW_HOME}
RUN mkdir -p ${AIRFLOW_HOME}/data && chown -R airflow: ${AIRFLOW_HOME}/data

# Switch to airflow user
USER airflow
WORKDIR ${AIRFLOW_HOME}

# CMD to run when container starts
CMD ["sh", "-c", "mkdir -p /opt/airflow/{dags,logs,plugins} && chown -R airflow: /opt/airflow && airflow standalone"] 