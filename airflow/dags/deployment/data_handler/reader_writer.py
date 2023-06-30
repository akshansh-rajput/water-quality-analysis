def data_reader(spark, options, data_format, source_location = None):
    df_reader = spark.read.format(data_format).options(**options)
    if source_location:
        return df_reader.load(source_location)
    else:
        return df_reader.load()
    
def data_writer(df, options, data_format, target = None):
    writer_df = df.write.format(data_format).options(**options)
    if target:
        writer_df.save(target)
    else:
        writer_df.save()
