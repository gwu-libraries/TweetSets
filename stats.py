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

SOURCE_DATASET_SQL = '''
select s1.dataset_id, s1.all_count, s3.all_recent_count, s2.local_count, s4.local_recent_count
from 
    (select 
         dataset_id, count(dataset_id) as all_count
    from source_datasets 
    group by dataset_id ) s1
left join (
    select dataset_id, count(dataset_id) as local_count
        from source_datasets
        where is_local=1 
        group by dataset_id 
    ) s2
on s1.dataset_id = s2.dataset_id
left join (
    select dataset_id, count(dataset_id) as all_recent_count
    from source_datasets 
    where create_timestamp >= (?) 
    group by dataset_id 
) s3
on s1.dataset_id = s3.dataset_id
left join (
    select dataset_id, count(dataset_id) as local_recent_count
    from source_datasets 
    where is_local=1 and create_timestamp >= (?) 
    group by dataset_id 
) s4
on s1.dataset_id = s4.dataset_id
order by s1.all_count desc
'''

DERIVATIVE_SQL = '''
select s1.derivative_type, s1.all_count, s3.all_recent_count, s2.local_count, s4.local_recent_count
from 
    (select 
         derivative_type, count(derivative_type) as all_count
    from derivatives 
    group by derivative_type ) s1
left join (
    select derivative_type, count(derivative_type) as local_count
        from derivatives
        where is_local=1 
        group by derivative_type
    ) s2
on s1.derivative_type = s2.derivative_type
left join (
    select derivative_type, count(derivative_type) as all_recent_count
    from derivatives 
    where create_timestamp >= (?) 
    group by derivative_type 
) s3
on s1.derivative_type = s3.derivative_type
left join (
    select derivative_type, count(derivative_type) as local_recent_count
    from derivatives 
    where is_local=1 and create_timestamp >= (?) 
    group by derivative_type
) s4
on s1.derivative_type = s4.derivative_type
order by s1.all_count desc

'''



class TweetSetStats():
    def __init__(self, db_filepath='/tweetsets_data/datasets/stats.db'):
        self._connection_cache = {}
        self.db_filepath = db_filepath
        # db_exists = os.path.exists(db_filepath)

        # Create db if it doesn't exist
        # if not db_exists:
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

    def source_datasets_stats(self, since_datetime, limit=100):
        """
        Returns stats table for source datasets
        """
        params = [since_datetime, since_datetime]
        return list(map(SourceDatasetMergeStat._make, self._get_conn().execute(SOURCE_DATASET_SQL, params).fetchmany(limit)))
    

    def derivatives_stats(self, since_datetime):
        """
        Returns stats table for derivatives.
        """
        params = [since_datetime, since_datetime]
        return list(map(DerivativeMergeStat._make, self._get_conn().execute(DERIVATIVE_SQL, params).fetchall()))


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
