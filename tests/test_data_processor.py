import unittest
from pyspark.sql import SparkSession
from src.data_processor import group_by_column_and_avg, join_dataframes

class TestDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_group_by_column_and_avg(self):
        file_path = "tests/test_data.csv"  
        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        result = group_by_column_and_avg(df, 'name', 'age')

        self.assertIsNotNone(result)
        self.assertEqual(result.count(), 3)  
        self.assertAlmostEqual(result.filter(result['name'] == 'Alice').select('avg_age').head()[0], 30.0)

    def test_join_dataframes(self):
        left_data = [
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')
        ]
        right_data = [
            (1, 'Engineer'),
            (2, 'Doctor'),
            (4, 'Artist')
        ]

        left_df = self.spark.createDataFrame(left_data, ['id', 'name'])
        right_df = self.spark.createDataFrame(right_data, ['id', 'profession'])

        result = join_dataframes(left_df, right_df, 'id')

        self.assertIsNotNone(result)
        self.assertEqual(result.count(), 3)    
        self.assertEqual(result.filter(result['name'] == 'Bob').select('profession').head()[0], 'Doctor')


