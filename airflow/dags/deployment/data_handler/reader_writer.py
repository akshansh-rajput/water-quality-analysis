
def data_reader(spark, data_format, options = None, source_location = None):
    """
    Spark read method, accept source information i.e. format, options, path and return DataFrame.
    Args:
        spark           : Spark session to use spark functionality 
        data_format     : Source format i.e. csv, kafka, json etc
        options         : extra parameters to use by spark while reading from source
        source_location : Source location. None in case kafka is source
    Return:
        DataFrame       : Spark Dataframe
    """
    df_reader = spark.read.format(data_format)
    if options:
        df_reader = df_reader.options(**options)
    if source_location:
        return df_reader.load(source_location)
    else:
        return df_reader.load()
    
def data_writer(df, data_format, partitionBy = None, options = None, target = None):
    """
    Spark writer method, accept target information i.e. format, options, path and write data into the sink.
    Args:
        df              : Data to write 
        data_format     : target format i.e. csv, kafka, json etc
        partitionBy     : Which column should be use for partitioning 
        options         : extra parameters to use by spark while writing to source
        target          : target location. None in case kafka is target
    """
    writer_df = df.write.format(data_format)
    if partitionBy:
        partition_cols = partitionBy.split(',')
        writer_df = writer_df.partitionBy(*partition_cols)
    if options:
        writer_df = writer_df.options(**options)
    if target:
        writer_df.mode('overwrite').save(target)
    else:
        writer_df.save()
