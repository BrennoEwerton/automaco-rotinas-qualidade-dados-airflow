# Dockerfile customizado para Airflow com Great Expectations
FROM apache/airflow:2.9.3

# Troca para o usuário airflow (recomendado)
USER airflow

# Instala Great Expectations no ambiente do usuário airflow
RUN pip install --no-cache-dir great_expectations==0.18.12 openai==1.52.2

