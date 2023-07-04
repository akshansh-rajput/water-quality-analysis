from pyspark.sql import SparkSession
from pyspark.sql.dataframe import StructType
import json
import os
from deployment.data_handler.reader_writer import data_reader, data_writer
from deployment.data_handler.transformations.transformation import filter_data, convert_to_json, remove_cols, selected_cols, extract_from_json, select_expr, inner_join, custom_expr

transformation_map = {
    'filter_data': filter_data,
    'to_json': convert_to_json,
    'remove_cols': remove_cols,
    'selected_cols': selected_cols,
    'extract_from_json': extract_from_json,
    'expr': select_expr,
    'custom_expr': custom_expr
}

def get_schema(path):
    with open(path) as sch_file:
        json_schema = json.load(sch_file)
        schema = StructType.fromJson(json_schema)
        return schema

def get_spark_session():
    #Adding kafka-spark package at runtime
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:2.0.0 pyspark-shell'
    spark = SparkSession \
            .builder \
            .appName("water-quality") \
            .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[*]") \
            .getOrCreate()
    return spark
    
def apply_transformation(df, transformations):
    for transformation in transformations:
            trans_name = transformation['name']
            extra_params = transformation['params']
            df = df.transform(transformation_map.get(trans_name), extra_params)
    return df

def data_reader_modification(spark, source_format, reading_options, first_source_location, config):
    df = data_reader(spark, source_format, reading_options, first_source_location)
    if source_format == 'kafka':
        schema_file = config.get('schema_file')
        schema = get_schema(schema_file)
        df = df.transform(select_expr, 'CAST(value as String)')
        df = df.transform(extract_from_json, schema)
    return df


def build_pipeline(config):
    """
    ETL/EL pipeline builder method. It build pipeline base on config provided in config file i.e. nature of pipeline
    , functions and flow.
    Args:
        config (dict): Complete details about data flow, transformation and sink details
    """
    spark = get_spark_session()
    #extracting config from config file
    first_source_location = config.get('source_location', None)
    sec_source_location = None
    target = config.get('target_location',None)
    target_format = config['target_format']
    target_options = config.get('target_options', None)
    source_format = config['source_format']
    reading_options = config.get('source_options', None)
    need_aggregation_data = config.get('req_secondary_data', False)
    partitionby = config.get('partitionBy', None)
    #Reading main source data
    df = data_reader_modification(spark, source_format, reading_options, first_source_location, config)
    sec_data = None
    #Read Data required for aggregation i.e joins
    if need_aggregation_data:
        pass
    #Apply transformation on data before aggregation if required
    if config.get('pre_transformation', None):
        transformations_before = config.get('pre_transformation', None)
        df = apply_transformation(df, transformations_before)

    #Apply aggregation if required
    if config.get('aggregation', None):
        agg_config = config.get('aggregation', None)
        if agg_config['type'] == 'inner_join':
            sec_data_format = agg_config['sec_data_format']
            sec_data_location = agg_config.get('sec_data_location', None)
            sec_data_options = agg_config.get('sec_data_options', None)
            sec_data = data_reader_modification(spark, sec_data_format, sec_data_options, sec_data_location, agg_config)
            df = inner_join(df, sec_data, agg_config['condition'])
    #Apply final transformation on data if required
    if config.get('post_transformation'):
        transformations_after = config.get('post_transformation', None)
        df = apply_transformation(df, transformations_after)

    #Sink final data
    if target_format == "kafka":
        df = df.transform(convert_to_json, 'value')
    data_writer(df, target_format, partitionby, target_options, target)
    