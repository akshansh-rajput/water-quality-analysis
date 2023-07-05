from pyspark.sql.functions import col, struct, to_json, from_json, expr

def filter_data(df, condition):
    """
    Helper method which apply filter transformation on dataframe
    Args:
        df          : Dataframe
        condition   : COndition for filtering
    Return:
        Dataframe
    """
    return df.filter(condition)

def convert_to_json(df, col_name):
    """
    Helper method which apply to_json transformation on dataframe
    Args:
        df          : Dataframe
        col_name    : alias column name after applying to_json operation
    Return:
        Dataframe
    """
    return df.select(to_json(struct('*')).alias(col_name))

def remove_cols(df, cols):
    """
    Helper method which drop columns from dataframe
    Args:
        df          : Dataframe
        cols        : Column list needs to remove from dataframe
    Return:
        Dataframe
    """
    columns = cols.split(',')
    return df.drop(*columns)

def selected_cols(df, cols):
    """
    Helper method which apply select operation on dataframe
    Args:
        df     : Dataframe
        cols   : Column to select
    Return:
        Dataframe
    """
    columns = cols.split(',')
    return df.select(*columns)

def select_expr(df, operation):
    """
    Helper method which apply custom exper transformaton on dataframe
    Args:
        df          : Dataframe
        operation   : expers which needs to apply on DF
    Return:
        Dataframe
    """
    return df.selectExpr(operation)

def extract_from_json(df, schema):
    """
    Helper method which extract column from json string
    Args:
        df       : Dataframe
        schema   : Schema of data which needs to be extract from json string
    Return:
        Dataframe
    """
    return df.select(from_json(col("value"), schema).alias("plateform_inter_data")).select("plateform_inter_data.*")

def inner_join(left_df, right_df, col):
    """
    Helper method which apply inner join on dataframe
    Args:
        left_df    : first Dataframe
        right_df   : second Dataframe
        col        : Column to use from inner join
    Return:
        Dataframe
    """
    return left_df.join(right_df, col)

def custom_expr(df, params):
    """
    Helper method which custom  exper transformaton on dataframe
    Args:
        df       : Dataframe
        params   : New Column name and expr which needs to be apply on Dataframe
    Return:
        Dataframe
    """
    new_col_name = params.get('col_name')
    exprs_str = params.get('expr')
    return df.withColumn(new_col_name, expr(exprs_str))
