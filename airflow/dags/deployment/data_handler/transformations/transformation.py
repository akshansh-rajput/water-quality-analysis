from pyspark.sql.functions import col, struct, to_json, from_json

def filter_data(df, condition):
    return df.filter(condition)

def convert_to_json(df, col_name):
    return df.select(to_json(struct('*')).alias(col_name))

def remove_cols(df, cols):
    columns = cols.split(',')
    return df.drop(*columns)

def selected_cols(df, cols):
    columns = cols.split(',')
    return df.select(*columns)

def expr(df, operation):
    return df.selectExpr(operation)

def extract_from_json(df, schema):
    return df.select(from_json(col("value"), schema).alias("plateform_inter_data")).select("plateform_inter_data.*")