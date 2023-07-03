def data_reader(spark, data_format, options = None, source_location = None):
    df_reader = spark.read.format(data_format)
    if options:
        df_reader = df_reader.options(**options)
    if source_location:
        return df_reader.load(source_location)
    else:
        return df_reader.load()
    
def data_writer(df, data_format, options = None, target = None):
    writer_df = df.write.format(data_format)
    if options:
        writer_df = writer_df.options(**options)
    if target:
        writer_df.mode('overwrite').save(target)
    else:
        writer_df.save()
