def read_data(spark, file_path, file_format='csv', header=True, infer_schema=True):
    format_options = {
        'csv': {
            'header': header,
            'inferSchema': infer_schema
        },
        'parquet': {},
        'json': {}
    }

    if file_format not in format_options:
        raise ValueError(f"Unsupported file format: {file_format}")

    return spark.read.format(file_format).options(**format_options[file_format]).load(file_path)


