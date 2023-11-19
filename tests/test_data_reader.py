import unittest
from pyspark.sql import SparkSession
from src.data_reader import read_data

class TestDataReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_read_test_csv(self):
        file_path = "tests/test_data.csv"
        df = read_data(self.spark, file_path)
        self.assertIsNotNone(df)
        expected_data = [
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 28),
            (4, 'Alice', 35),
        ]
        self.assertEqual(sorted(df.collect()), sorted(expected_data))

    def test_read_test_json(self):
        file_path = "tests/test_data.json"
        df = read_data(self.spark, file_path, file_format='json')
        self.assertIsNotNone(df)
        expected_data = [
            (25, 1, 'Alice'),
            (30, 2, 'Bob'),
            (28, 3, 'Charlie')
        ]
        self.assertEqual(sorted(df.collect()), sorted(expected_data))



