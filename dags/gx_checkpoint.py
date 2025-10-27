from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.data_context import DataContext

def run_gx_checkpoint():
    context = DataContext("/opt/airflow/gx")
    checkpoint_name = "microdados_2022.CSV_microdados_suite"
    
    checkpoint = SimpleCheckpoint(checkpoint_name, context)
    result = checkpoint.run()
    
    # Gera o Data Docs localmente
    context.build_data_docs()
    
    # Retorna True ou False
    return result["success"]

with DAG(
    dag_id="microdados_validation",
    start_date=datetime(2025, 10, 6),
    schedule_interval="@daily",
    catchup=False
) as dag:

    gx_validation = PythonOperator(
        task_id="run_gx_checkpoint",
        python_callable=run_gx_checkpoint
    )