from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from deployment.data_handler.pipeline_builder import build_pipeline
@task.python
def pipeline_builder(config):
    build_pipeline(config)
    # import os
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'
    # print("==================================")
    # spark = SparkSession.builder.appName("test_data_explore").master("local[*]").getOrCreate()
    # df = spark.read.format('csv').option("header","true").load('/opt/airflow/dags/input/data/Waterbase_v2021_1_T_WISE6_DisaggregatedData.csv')
    # df.select(to_json(struct('*')).alias('value')).write.format("kafka").option("kafka.bootstrap.servers", "broker:29092").option("topic", "test_air").save()