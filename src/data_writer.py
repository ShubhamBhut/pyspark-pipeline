def write_data(data, file_path, file_format='parquet', compression=None):
    writer = data.write.format(file_format).mode("overwrite")
    if compression:
        writer.option("compression", compression)
    writer.save(file_path)

