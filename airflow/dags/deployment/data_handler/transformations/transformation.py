from pyspark.sql.functions import col, struct, to_json

def filter_data(df, condition):
    return df.filter(condition)

def convert_to_json(df, col_name):
    return df.select(to_json(struct('*')).alias(col_name))