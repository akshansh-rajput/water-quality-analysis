from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
import yaml
import os

for filename in os.listdir('/opt/airflow/dags/config/'):
    if filename.endswith('.yml'):
        with open(f'/opt/airflow/dags/config/{filename}') as fi:
            conf_file = yaml.safe_load(fi)

            dag_name = conf_file['dag_name']

            with DAG(
                dag_id=dag_name,
                default_args={
                    'retries': 0
                },
                schedule='0 0 * * *',
                start_date=datetime(2022, 10, 1),
                catchup=False
            ) as dag_1:

                from deployment.task.task import pipeline_builder
                t_data = []
                data_processors = conf_file['data_processor']
                for data_processor in data_processors:
                    task_name = data_processor['name']
                    data_task = pipeline_builder.override(task_id=task_name)(data_processor)
                    t_data.append(data_task)
                need_kafka_topic = conf_file['need_kafka_topic']
                if need_kafka_topic:
                    topic_name = conf_file['topic_name']
                    server = conf_file['kafka_server']
                    notebook_task = PapermillOperator(
                        task_id=f'create_topic',
                        input_nb="dags/deployment/kafka_manager.ipynb",
                        output_nb="dags/deployment/output/kafka_manager.ipynb",
                        parameters={"KAFKA_BROKER_SERVER": server,"KAFKA_TOPIC_NAME":topic_name},
                    )
                    notebook_task >> t_data

            