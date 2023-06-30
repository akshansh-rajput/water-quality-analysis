from pyspark.sql import SparkSession
import os
from deployment.data_handler.reader_writer import data_reader, data_writer
from deployment.data_handler.transformations.transformation import filter_data, convert_to_json

transformation_map = {
    'filter_data': filter_data,
    'to_json': convert_to_json
}

def build_pipeline(config):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'
    print(config)
    spark = SparkSession.builder.appName("water-quality").master("local[*]").getOrCreate()
    first_source_location = config['feed1_source']
    sec_source_location = None
    # feed_type = config['feed_type']
    target = config.get('target_location',None)
    target_format = config['target_format']
    target_options = config['target_options']
    data_format = config['data_type']
    reading_options = config['feed_options']
    df = data_reader(spark, reading_options, data_format, first_source_location)
    if config.get('pre_transformation', None):
        transformations_before = config.get('pre_transformation', None)
        for trans in transformations_before:
            trans_name = trans['name']
            extra_params = trans['condition']
            df = df.transform(transformation_map.get(trans_name), extra_params)
    if config.get('aggregation'):
        pass
    if config.get('post_transformation'):
        transformations_after = config.get('post_transformation', None)
        for trans in transformations_after:
            trans_name = trans['name']
            extra_params = trans['condition']
            df = df.transform(transformation_map.get(trans_name), extra_params)
    
    data_writer(df, target_options, target_format, target)
    