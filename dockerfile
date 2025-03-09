FROM apache/airflow:2.7.1

USER root

# Installing system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Creation of the necessary files
RUN mkdir -p /opt/airflow/build /opt/airflow/data/raw /opt/airflow/scripts
RUN chown -R airflow:root /opt/airflow/build /opt/airflow/data/raw /opt/airflow/scripts

USER airflow

# Copy requirements and installation
COPY build/reqs.txt /opt/airflow/build/reqs.txt
RUN pip install -r /opt/airflow/build/reqs.txt

