from pyspark.sql import SparkSession
import os
from deployment.data_handler.reader_writer import data_reader, data_writer
def build_pipeline(config):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'
    
    spark = SparkSession.builder.appName("water-quality").master("local[*]").getOrCreate()
    first_source_location = config['feed1_source']
    sec_source_location = None
    feed_type = config['feed_type']
    target = config['target_location']
    target_format = config['target_format']
    target_options = config['target_options']
    data_format = config['data_type']
    reading_options = config['feed_options']
    df = data_reader(spark, reading_options, data_format, first_source_location)
    if config.get('pre_transformation'):
        pass
    if config.get('aggregation'):
        pass
    if config.get('post_transformation'):
        pass
    
    
    