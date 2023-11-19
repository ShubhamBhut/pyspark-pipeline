from pyspark.sql.functions import col, avg

def group_by_column_and_avg(data, group_column, value_column):
    return data.groupBy(group_column).agg(avg(col(value_column)).alias(f"avg_{value_column}"))

def join_dataframes(left_df, right_df, join_column, join_type='left'):
    return left_df.join(right_df, on=join_column, how=join_type)
