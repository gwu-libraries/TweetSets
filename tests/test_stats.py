from unittest import TestCase
import tempfile
import os
import shutil
from stats import TweetSetStats
from datetime import datetime
from time import sleep


class TestTweetSetStats(TestCase):

    def setUp(self):
        self.db_path = tempfile.mkdtemp()
        self.stats = TweetSetStats(db_filepath=os.path.join(self.db_path, 'stat.db'))

    def tearDown(self):
        shutil.rmtree(self.db_path, ignore_errors=True)

    def test_datasets(self):
        before = datetime.utcnow()
        sleep(1)
        self.stats.add_dataset(True, 10)
        sleep(1)
        middle = datetime.utcnow()
        sleep(1)
        self.stats.add_dataset(False, 20)
        after = datetime.utcnow()
        self.assertEqual((2, 30), self.stats.datasets_stats())
        self.assertEqual((1, 10), self.stats.datasets_stats(local_only=True))
        self.assertEqual((2, 30), self.stats.datasets_stats(since_datetime=before))
        self.assertEqual((1, 20), self.stats.datasets_stats(since_datetime=middle))
        self.assertEqual((0, None), self.stats.datasets_stats(local_only=True, since_datetime=middle))
        self.assertEqual((0, None), self.stats.datasets_stats(since_datetime=after))

    def test_source_datasets(self):
        self.stats.add_source_dataset('b', True)
        self.stats.add_source_dataset('a', True)
        self.stats.add_source_dataset('a', False)

        self.assertEqual([('a', 2), ('b', 1)], self.stats.source_datasets_stats())
        self.assertEqual([('a', 2)], self.stats.source_datasets_stats(limit=1))
        self.assertEqual([('a', 1), ('b', 1)], self.stats.source_datasets_stats(local_only=True))

    def test_derivatives(self):
        self.stats.add_derivative('b', True)
        self.stats.add_derivative('a', True)
        self.stats.add_derivative('a', False)

        self.assertEqual([('a', 2), ('b', 1)], self.stats.derivatives_stats())
        self.assertEqual([('a', 2), ('b', 1)], self.stats.derivatives_stats())
        self.assertEqual([('a', 1), ('b', 1)], self.stats.derivatives_stats(local_only=True))
