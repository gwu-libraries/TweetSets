import os
import sqlite3
from threading import get_ident
from collections import namedtuple

DatasetStat = namedtuple('DatasetStat', ['count', 'tweets'])
SourceDatasetStat = namedtuple('SourceDatasetStat', ['dataset_id', 'count'])
SourceDatasetMergeStat = namedtuple('SourceDatasetMergeStat',
                                    ['dataset_id', 'all_count', 'all_recent_count', 'local_count',
                                     'local_recent_count'])
DerivativeStat = namedtuple('DerivativeStat', ['derivative_type', 'count'])
DerivativeMergeStat = namedtuple('DerivativeMergeStat',
                                 ['derivative_type', 'all_count', 'all_recent_count', 'local_count',
                                  'local_recent_count'])


class TweetSetStats():
    def __init__(self, db_filepath='/tweetsets_data/datasets/stats.db'):
        self._connection_cache = {}
        self.db_filepath = db_filepath
        db_exists = os.path.exists(db_filepath)

        # Create db if it doesn't exist
        if not db_exists:
            self._create_db()

    def _get_conn(self):
        id = get_ident()
        if id not in self._connection_cache:
            self._connection_cache[id] = sqlite3.connect(self.db_filepath,
                                                         detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

        return self._connection_cache[id]

    def _create_db(self):
        conn = self._get_conn()
        with conn:
            conn.execute(
                'create table if not exists datasets (create_timestamp timestamp default current_timestamp, is_local '
                'boolean, tweet_count integer);')
            conn.execute(
                'create table if not exists source_datasets (create_timestamp timestamp default current_timestamp, '
                'dataset_id, is_local boolean);')
            conn.execute(
                'create table if not exists derivatives (create_timestamp timestamp default current_timestamp, '
                'derivative_type, is_local boolean);')

    def add_dataset(self, is_local, tweet_count):
        conn = self._get_conn()
        with conn:
            conn.execute('insert into datasets (is_local, tweet_count) values (?, ?);', (is_local, tweet_count))

    def add_source_dataset(self, dataset_id, is_local):
        conn = self._get_conn()
        with conn:
            conn.execute('insert into source_datasets (is_local, dataset_id) values (?, ?);',
                         (is_local, dataset_id))

    def add_derivative(self, derivative_type, is_local):
        conn = self._get_conn()
        with conn:
            conn.execute('insert into derivatives (is_local, derivative_type) values (?, ?);',
                         (is_local, derivative_type))

    def datasets_stats(self, local_only=False, since_datetime=None):
        """
        Returns (count of datasets, sum of tweets)
        """
        sql = 'select count(create_timestamp), sum(tweet_count) from datasets'
        params, params_sql = self._params(local_only, since_datetime)
        if params_sql:
            sql += ' where {}'.format(' and '.join(params_sql))
        return DatasetStat._make(self._get_conn().execute(sql, params).fetchone())

    def source_datasets_stats(self, local_only=False, since_datetime=None, limit=10):
        """
        Returns [(dataset_id, count of datasets), ...]
        """
        sql1 = 'select dataset_id, count(dataset_id) from source_datasets'
        sql2 = 'group by dataset_id order by count(dataset_id) desc'
        params, params_sql = self._params(local_only, since_datetime)
        sql = '{} {}'.format(sql1, sql2)
        if params_sql:
            sql = '{} where {} {}'.format(sql1, ' and '.join(params_sql), sql2)
        return list(map(SourceDatasetStat._make, self._get_conn().execute(sql, params).fetchmany(limit)))

    def source_datasets_merge_stats(self, since_datetime=None):
        """
        Returns [(dataset_id, all count of datasets, all recent count, local count, local recent count), ...]
        """
        source_datasets = {}
        for source_dataset, count in self.source_datasets_stats():
            if source_dataset not in source_datasets:
                source_datasets[source_dataset] = SourceDatasetMergeStat(source_dataset, 0, 0, 0, 0)
            source_datasets[source_dataset] = source_datasets[source_dataset]._replace(all_count=count)
        for source_dataset, count in self.source_datasets_stats(local_only=True):
            if source_dataset not in source_datasets:
                source_datasets[source_dataset] = SourceDatasetMergeStat(source_dataset, 0, 0, 0, 0)
                source_datasets[source_dataset] = source_datasets[source_dataset]._replace(local_count=count)
        for source_dataset, count in self.source_datasets_stats(since_datetime=since_datetime):
            source_datasets[source_dataset] = source_datasets[source_dataset]._replace(all_recent_count=count)
        for source_dataset, count in self.source_datasets_stats(local_only=True, since_datetime=since_datetime):
            source_datasets[source_dataset] = source_datasets[source_dataset]._replace(local_recent_count=count)
        return list(source_datasets.values())


    def derivatives_stats(self, local_only=False, since_datetime=None):
        """
        Returns [(derivative type, count of datasets), ...]
        """
        sql1 = 'select derivative_type, count(derivative_type) from derivatives'
        sql2 = 'group by derivative_type order by count(derivative_type) desc'
        params, params_sql = self._params(local_only, since_datetime)
        sql = '{} {}'.format(sql1, sql2)
        if params_sql:
            sql = '{} where {} {}'.format(sql1, ' and '.join(params_sql), sql2)
        return list(map(DerivativeStat._make, self._get_conn().execute(sql, params).fetchall()))

    def derivatives_merge_stats(self, since_datetime=None):
        """
        Returns [(derivative type, all count of datasets, all recent count, local count, local recent count), ...]
        """
        derivatives = {}
        for derivative_type, count in self.derivatives_stats():
            if derivative_type not in derivatives:
                derivatives[derivative_type] = DerivativeMergeStat(derivative_type, 0, 0, 0, 0)
            derivatives[derivative_type] = derivatives[derivative_type]._replace(all_count=count)
        for derivative_type, count in self.derivatives_stats(local_only=True):
            if derivative_type not in derivatives:
                derivatives[derivative_type] = DerivativeMergeStat(derivative_type, 0, 0, 0, 0)
                derivatives[derivative_type] = derivatives[derivative_type]._replace(local_count=count)
        for derivative_type, count in self.derivatives_stats(since_datetime=since_datetime):
            derivatives[derivative_type] = derivatives[derivative_type]._replace(all_recent_count=count)
        for derivative_type, count in self.derivatives_stats(local_only=True, since_datetime=since_datetime):
            derivatives[derivative_type] = derivatives[derivative_type]._replace(local_recent_count=count)
        return list(derivatives.values())

    @staticmethod
    def _params(local_only, since_datetime):
        params = []
        params_sql = []
        if local_only:
            params_sql.append('is_local=(?)')
            params.append(True)
        if since_datetime:
            params_sql.append('create_timestamp >= (?)')
            params.append(since_datetime)
        return params, params_sql
