from pyspark.sql import SparkSession
from data_reader import read_data
from data_processor import group_by_column_and_avg, join_dataframes
from data_writer import write_data

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HousingPriceAnalysis") \
        .getOrCreate()

    data_path = "data/cali_housing_old.csv" 
    housing_data = read_data(spark, data_path)
    housing_data.show(5)

    new_data = read_data(spark, "data/cali_housing_new.csv")
    joined_data = join_dataframes(housing_data, new_data, "id")
    joined_data.show(5)

    avg_price_by_location = group_by_column_and_avg(joined_data, "ocean_proximity", "median_house_value")
    avg_price_by_location.show(5)

    write_data(avg_price_by_location, "output/avg_price_by_location", file_format='parquet', compression='gzip')
    write_data(avg_price_by_location, "output/json_data", file_format='json')

    json_data = read_data(spark, "output/json_data", file_format='json')
    json_data.show()

    spark.stop()

