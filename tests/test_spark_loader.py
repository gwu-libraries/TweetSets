import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, explode
import pyspark.sql.functions as F
from spark_utils import *
import json
import unittest
from .tweets_2_2 import tests, tests_csv
import os



class TestTweet(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName('TweetSets-Test').getOrCreate()
        self.spark.conf.set('spark.sql.session.timeZone', 'UTC')
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
        self.ts_schema = load_schema('./tweetsets_schema.json')
        self.ts_sql = load_sql('./tweetsets_sql_exp.sql')
        self.tests = tests
        self.tests_csv = tests_csv
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.df = make_spark_df(self.spark, 
                                schema=self.ts_schema, 
                                sql=self.ts_sql, 
                                path_to_dataset='file:///' + self.dir_path + '/spark-test-tweets.json',
                                dataset_id='test-df')

    def testDataFrame(self):
        for i, (row, test) in enumerate(zip(self.df.collect(), self.tests)):
            for k, v in row.asDict().items():
                self.assertEqual(v, test[k], msg=f'Tests failed on tweet number {i}, key={k}. Spark DF has {v}, test has {test[k]}.')
        return
    
    def testCSV(self):
        csv = extract_csv(self.df)
        for i, (row, test) in enumerate(zip(csv.collect(), self.tests_csv)):
            for k, v in row.asDict().items():
                self.assertEqual(v, test[k], msg=f'Tests failed on tweet number {i}, key={k}. Spark CSV has {v}, test_csv has {test[k]}.')
        return

if __name__ == '__main__':
    unittest.main()