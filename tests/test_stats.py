from unittest import TestCase
import tempfile
import os
import shutil
from stats import TweetSetStats, SourceDatasetMergeStat, DerivativeMergeStat
from datetime import datetime, timedelta
import random
from time import sleep
from collections import Counter

NUM_ROWS = 100

class TestTweetSetStats(TestCase):

    def setUp(self):
        self.db_path = tempfile.mkdtemp()
        self.stats = TweetSetStats(db_filepath=os.path.join(self.db_path, 'stat.db'))
        self.since = datetime.utcnow() - timedelta(days=30 * 6)

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
        dataset_ids = [random.choice('ABCD') for i in range(NUM_ROWS)]
        is_local = [random.choice([1, 0]) for i in range(NUM_ROWS)]
        source_stats = Counter()
        for d, l in zip(dataset_ids, is_local):
            self.stats.add_source_dataset(d, l)
            source_stats[d] += 1
        stats = self.stats.source_datasets_stats(self.since, limit=NUM_ROWS)
        for s in stats:
            self.assertEqual(len([d for d in dataset_ids if d == s.dataset_id]), s.all_count)
            self.assertEqual(source_stats[s.dataset_id], s.local_count)

    def test_derivatives(self):
        self.stats.add_derivative('b', True)
        self.stats.add_derivative('a', True)
        self.stats.add_derivative('a', False)

        self.assertEqual([('a', 2), ('b', 1)], self.stats.derivatives_stats())
        self.assertEqual([('a', 2), ('b', 1)], self.stats.derivatives_stats())
        self.assertEqual([('a', 1), ('b', 1)], self.stats.derivatives_stats(local_only=True))
