import unittest
import shutil
from pyspark.sql import SparkSession
from src.data_writer import write_data
from src.data_reader import read_data

class TestDataWriter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.output_dir = 'tests/test_output/'
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def tearDown(self):
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def test_write_to_parquet(self):
        data = [
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 28)
        ]
        df = self.spark.createDataFrame(data, ['id', 'name', 'age'])

        write_data(df, self.output_dir + 'test_parquet', file_format='parquet')

        written_df = read_data(self.spark, self.output_dir + 'test_parquet', file_format='parquet')

        self.assertEqual(df.count(), written_df.count())  

    def test_write_to_json(self):
        data = [
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 28)
        ]
        df = self.spark.createDataFrame(data, ['id', 'name', 'age'])

        write_data(df, self.output_dir + 'test_json', file_format='json')

        written_df = read_data(self.spark, self.output_dir + 'test_json', file_format='json')

        self.assertEqual(df.count(), written_df.count())          
