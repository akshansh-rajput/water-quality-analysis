from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator


with DAG(
    dag_id='example_papermill_operator',
    default_args={
        'retries': 0
    },
    schedule='0 0 * * *',
    start_date=datetime(2022, 10, 1),
    catchup=False
) as dag_1:

    notebook_task = PapermillOperator(
        task_id="run_example_notebook",
        input_nb="dags/deployment/kafka_manager.ipynb",
        output_nb="dags/deployment/output/kafka_manager.ipynb",
        parameters={"KAFKA_BROKER_SERVER": "broker:29092","KAFKA_TOPIC_NAME":"test_air"},
    )