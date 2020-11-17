from unittest import TestCase
import tempfile
import os
import shutil
from stats import TweetSetStats
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
        self.ids = [random.choice('ABCD') for i in range(NUM_ROWS)]
        self.is_local = [random.choice([1, 0]) for i in range(NUM_ROWS)]
        self.counts = Counter()

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
        counts = Counter()
        for d, l in zip(self.ids, self.is_local):
            self.stats.add_source_dataset(d, l)
            counts[d] += l
        stats = self.stats.source_datasets_stats(self.since, limit=NUM_ROWS)
        for s in stats:
            self.assertEqual(len([d for d in self.ids if d == s.dataset_id]), s.all_count)
            self.assertEqual(counts[s.dataset_id], s.local_count)

    def test_derivatives(self):
        counts = Counter()
        for d, l in zip(self.ids, self.is_local):
            self.stats.add_derivative(d, l)
            counts[d] += l
        stats = self.stats.derivatives_stats(self.since)
        for s in stats:
            self.assertEqual(len([d for d in self.ids if d == s.derivative_type]), s.all_count)
            self.assertEqual(counts[s.derivative_type], s.local_count)
