import os
import fnmatch
import sqlite3
import json
import csv
from utils import dataset_params_to_search
import logging
from twarc import json2csv
import zipfile

logger = logging.getLogger(__name__)


def generate_tasks(self, task_defs, dataset_params, total_tweets, dataset_path, generate_update_increment=None,
                   zip_bytes_threshold=1000000000):
    generate_update_increment = generate_update_increment or 10000

    tasks = []
    task_args = [self, total_tweets, dataset_path, generate_update_increment]
    for task_name, task_kwargs in task_defs.items():
        tasks.append(task_class_map[task_name](*task_args, **task_kwargs))
    search = dataset_params_to_search(dataset_params)
    source = set()

    for task in tasks:
        # Delete existing files
        if task.file_filter:
            # Unzipped files
            for filename in fnmatch.filter(os.listdir(dataset_path), task.file_filter):
                os.remove(os.path.join(dataset_path, filename))
            # Zipped files
            for filename in fnmatch.filter(os.listdir(dataset_path), '{}.zip'.format(task.file_filter)):
                os.remove(os.path.join(dataset_path, filename))

        task.on_start()
        source.update(task.source)

    if source:
        search.source(list(source))

    tweet_count = 0
    for tweet_count, hit in enumerate(search.scan()):
        # This is to support limiting the number of tweets
        if tweet_count + 1 > total_tweets:
            break

        for task in tasks:
            task.on_hit(hit, tweet_count)

        if (tweet_count + 1) % generate_update_increment == 0:
            self.update_state(state='PROGRESS',
                              meta={'current': tweet_count + 1, 'total': total_tweets,
                                    'status': '{:,d} of {:,d} tweet ids'.format(tweet_count + 1,
                                                                                total_tweets)})

    for task in tasks:
        task.on_end()

        # Zip files
        z = None
        zip_filepath = None
        file_count = 1
        for filename in sorted(fnmatch.filter(os.listdir(dataset_path), task.file_filter)):
            if z is None or os.path.getsize(zip_filepath) > zip_bytes_threshold:
                if z:
                    z.close()
                zip_filepath = os.path.join(dataset_path,
                                            '{}.zip'.format(task.file_filter.replace('*', str(file_count).zfill(3))))
                z = zipfile.ZipFile(zip_filepath, 'w', compression=zipfile.ZIP_DEFLATED)
                file_count += 1
            filepath = os.path.join(dataset_path, filename)
            z.write(filepath, arcname=filename)
            os.remove(os.path.join(dataset_path, filename))
        if z:
            z.close()

    generate_task_filepath = os.path.join(dataset_path, 'generate_tasks.json')
    if os.path.exists(generate_task_filepath):
        os.remove(generate_task_filepath)

    return {'current': tweet_count + 1, 'total': total_tweets,
            'status': 'Completed.'}


class BaseGenerateTask:
    def __init__(self, state, total_tweets, dataset_path, generate_update_increment, file_filter=None, source=None):
        self.state = state
        self.dataset_path = dataset_path
        self.total_tweets = total_tweets
        self.file_filter = file_filter
        self.source = source or []
        self.generate_update_increment = generate_update_increment

    def on_start(self):
        pass

    def on_hit(self, hit, tweet_count):
        raise NotImplementedError('on_hit must be implemented')

    def on_end(self):
        pass

    def update_state(self, current, total, status, state='PROGRESS'):
        self.state.update_state(state=state,
                                meta={'current': current, 'total': total,
                                      'status': status})


class GenerateTweetIdsTask(BaseGenerateTask):
    def __init__(self, *args, max_per_file=None):
        super(GenerateTweetIdsTask, self).__init__(*args, file_filter='tweet-ids-*.txt')

        self.max_per_file = max_per_file or 10000000
        self.file = None
        self.file_count = 1

    def on_hit(self, hit, tweet_count):
        # Cycle tweet id files
        if tweet_count % self.max_per_file == 0:
            if self.file:
                self.file.close()
            self.file = open(
                os.path.join(self.dataset_path, 'tweet-ids-{}.txt'.format(str(self.file_count).zfill(3))), 'w')
            self.file_count += 1
        # Write to tweet id file
        self.file.write(hit.meta.id)
        self.file.write('\n')

    def on_end(self):
        if self.file:
            self.file.close()


class GenerateTweetJSONTask(BaseGenerateTask):
    def __init__(self, *args, max_per_file=None):
        super(GenerateTweetJSONTask, self).__init__(*args, file_filter='tweets-*.jsonl', source=['tweet'])

        self.max_per_file = max_per_file or 10000000
        self.file = None
        self.file_count = 1

    def on_hit(self, hit, tweet_count):
        # Cycle tweet id files
        if tweet_count % self.max_per_file == 0:
            if self.file:
                self.file.close()
            self.file = open(
                os.path.join(self.dataset_path, 'tweets-{}.jsonl'.format(str(self.file_count).zfill(3))), 'w')
            self.file_count += 1
        # Write to tweet file
        self.file.write(hit.tweet)
        self.file.write('\n')

    def on_end(self):
        if self.file:
            self.file.close()


class GenerateTweetCSVTask(BaseGenerateTask):
    def __init__(self, *args, max_per_file=None):
        super(GenerateTweetCSVTask, self).__init__(*args, file_filter='tweets-*.csv', source=['tweet'])

        self.max_per_file = max_per_file or 250000
        self.file = None
        self.sheet = None
        self.file_count = 1

    def on_hit(self, hit, tweet_count):
        # Cycle tweet id files
        if tweet_count % self.max_per_file == 0:
            if self.file:
                self.file.close()
            self.file = open(
                os.path.join(self.dataset_path, 'tweets-{}.csv'.format(str(self.file_count).zfill(3))), 'w')
            self.sheet = csv.writer(self.file)
            self.sheet.writerow(json2csv.get_headings())
            self.file_count += 1
        # Write to tweet file
        self.sheet.writerow(json2csv.get_row(json.loads(hit.tweet), excel=True))

    def on_end(self):
        if self.file:
            self.file.close()


class GenerateMentionsTask(BaseGenerateTask):
    def __init__(self, *args, max_per_file=None):
        super(GenerateMentionsTask, self).__init__(*args, file_filter='mention-*.csv',
                                                   source=['mention_user_ids', 'user_id'])

        self.max_per_file = max_per_file or 10000000
        self.db_filepath = os.path.join(self.dataset_path, "mentions.db")
        self.edges_file = None
        self.nodes_file = None
        self.mention_count = 0
        self.conn = None
        self.file_count = 1

    def on_start(self):
        # Create db
        if os.path.exists(self.db_filepath):
            os.remove(self.db_filepath)
        self.conn = sqlite3.connect(self.db_filepath)
        with self.conn:
            self.conn.execute('create table user_ids (user_id primary key);')
        self.nodes_file = open(os.path.join(self.dataset_path, 'mention-nodes.csv'), 'w')

    def on_hit(self, hit, tweet_count):
        # Cycle edges files
        if tweet_count % self.max_per_file == 0:
            if self.edges_file:
                self.edges_file.close()
            self.edges_file = open(
                os.path.join(self.dataset_path, 'mention-edges-{}.csv'.format(str(self.file_count).zfill(3))), 'w')
            self.file_count += 1
        # Write to mentions to file
        if hasattr(hit, 'mention_user_ids'):
            for i, mention_user_id in enumerate(hit.mention_user_ids):
                # Encountered instances where mention_user_id is null.
                if mention_user_id:
                    self.mention_count += 1
                    # Write mention user id (edge)
                    self.edges_file.write(','.join([hit.user_id, mention_user_id]))
                    self.edges_file.write('\n')

                    # Possibly write mention user id to mention screen name (node)
                    try:
                        with self.conn:
                            self.conn.execute('insert into user_ids(user_id) values (?);', (mention_user_id,))
                        self.nodes_file.write(','.join([mention_user_id, hit.mention_screen_names[i]]))
                        self.nodes_file.write('\n')
                    except sqlite3.IntegrityError:
                        # A dupe, so skipping writing to nodes file
                        pass

    def on_end(self):
        if self.edges_file:
            self.edges_file.close()
        self.nodes_file.close()
        self.conn.close()
        os.remove(self.db_filepath)


class GenerateTopMentionsTask(BaseGenerateTask):
    def __init__(self, *args, max_per_file=None):
        super(GenerateTopMentionsTask, self).__init__(*args, file_filter='top-mentions-*.csv',
                                                      source=['mention_user_ids', 'mention_screen_names'])

        self.max_per_file = max_per_file or 250000
        self.db_filepath = os.path.join(self.dataset_path, "top-mentions.db")
        self.conn = None
        self.mention_count = 0
        self.total_user_count = 0
        self.count_buf = dict()
        self.user_buf = set()

    def on_start(self):
        # Create db
        if os.path.exists(self.db_filepath):
            os.remove(self.db_filepath)
        self.conn = sqlite3.connect(self.db_filepath)
        with self.conn:
            self.conn.execute('create table mentions(user_id primary key, mention_count int);')
            self.conn.execute('create table users(user_id int primary key, screen_name text);')
            self.conn.execute('create unique index users_idx on users(user_id, screen_name);')

    def on_hit(self, hit, tweet_count):
        if hasattr(hit, 'mention_user_ids'):
            for i, mention_user_id in enumerate(hit.mention_user_ids):
                # Encountered unexpected blank user ids
                if mention_user_id:
                    self.mention_count += 1
                    mention_screen_name = hit.mention_screen_names[i]

                    if mention_user_id in self.count_buf:
                        self.count_buf[mention_user_id] += 1
                    else:
                        cur = self.conn.cursor()
                        cur.execute('update mentions set mention_count=mention_count+1 where user_id=?',
                                    (mention_user_id,))
                        if not cur.rowcount:
                            self.count_buf[mention_user_id] = 1
                        self.conn.commit()

                    self.user_buf.add((mention_user_id, mention_screen_name))

                    if len(self.count_buf) and len(self.count_buf) % 1000 == 0:
                        with self.conn:
                            self.conn.executemany(
                                'insert into mentions(user_id, mention_count) values (?, ?);',
                                _mention_iter(self.count_buf))
                        self.total_user_count += len(self.count_buf)
                        self.count_buf = dict()

                    if len(self.user_buf) and len(self.user_buf) % 1000 == 0:
                        with self.conn:
                            self.conn.executemany(
                                'insert or ignore into users(user_id, screen_name) values (?, ?);',
                                self.user_buf)
                        self.user_buf = set()

    def on_end(self):
        # Final write of buffer
        if len(self.count_buf):
            with self.conn:
                self.conn.executemany('insert into mentions(user_id, mention_count) values (?, ?);',
                                      _mention_iter(self.count_buf))
            self.total_user_count += len(self.count_buf)
        if len(self.user_buf):
            with self.conn:
                self.conn.executemany('insert or ignore into users(user_id, screen_name) values (?, ?);',
                                      self.user_buf)

        file_count = 1
        file = None
        try:
            cur = self.conn.cursor()
            for user_count, row in enumerate(
                    cur.execute("select user_id, mention_count from mentions order by mention_count desc")):
                user_id = row[0]
                mention_count = row[1]
                # Cycle tweet id files
                if user_count % self.max_per_file == 0:
                    if file:
                        file.close()
                    file = open(
                        os.path.join(self.dataset_path, 'top-mentions-{}.csv'.format(str(file_count).zfill(3))), 'w')
                    file_count += 1
                # Get screen names
                screen_names = []
                for user_row in self.conn.execute("select screen_name from users where user_id=?", (user_id,)):
                    screen_names.append(user_row[0])

                # Write to mentions to file
                line = [user_id, str(mention_count)]
                line.extend(screen_names)
                file.write(','.join(line))
                file.write('\n')

                if (user_count + 1) % self.generate_update_increment == 0:
                    self.update_state(user_count + 1, self.total_user_count,
                                      '{:,d} of {:,d} mentioners in {:,d} files'.format(
                                          user_count + 1, self.total_user_count, file_count))
        finally:
            if file:
                file.close()
        os.remove(self.db_filepath)


class GenerateTopUsersTask(BaseGenerateTask):
    def __init__(self, *args, max_per_file=None):
        super(GenerateTopUsersTask, self).__init__(*args, file_filter='top-users-*.csv',
                                                   source=['user_id', 'user_screen_name'])

        self.max_per_file = max_per_file or 250000
        self.db_filepath = os.path.join(self.dataset_path, "top-users.db")
        self.conn = None
        self.total_user_count = 0
        self.count_buf = dict()
        self.user_buf = set()

    def on_start(self):
        # Create db
        if os.path.exists(self.db_filepath):
            os.remove(self.db_filepath)
        self.conn = sqlite3.connect(self.db_filepath)
        with self.conn:
            self.conn.execute('create table tweets(user_id primary key, tweet_count int);')
            self.conn.execute('create table users(user_id int primary key, screen_name text);')
            self.conn.execute('create unique index users_idx on users(user_id, screen_name);')

    def on_hit(self, hit, tweet_count):
        screen_name = hit.user_screen_name
        user_id = hit.user_id
        if user_id in self.count_buf:
            self.count_buf[user_id] += 1
        else:
            cur = self.conn.cursor()
            cur.execute('update tweets set tweet_count=tweet_count+1 where user_id=?',
                        (user_id,))
            if not cur.rowcount:
                self.count_buf[user_id] = 1
                self.conn.commit()

                self.user_buf.add((user_id, screen_name))

        if len(self.count_buf) and len(self.count_buf) % 1000 == 0:
            with self.conn:
                self.conn.executemany(
                    'insert into tweets(user_id, tweet_count) values (?, ?);', self.count_buf.items())
                self.total_user_count += len(self.count_buf)
                self.count_buf = dict()

        if len(self.user_buf) and len(self.user_buf) % 1000 == 0:
            with self.conn:
                self.conn.executemany(
                    'insert or ignore into users(user_id, screen_name) values (?, ?);',
                    self.user_buf)
                self.user_buf = set()

    def on_end(self):
        # Final write of buffer
        if len(self.count_buf):
            with self.conn:
                self.conn.executemany('insert into tweets(user_id, tweet_count) values (?, ?);', self.count_buf.items())
                self.total_user_count += len(self.count_buf)
        if len(self.user_buf):
            with self.conn:
                self.conn.executemany('insert or ignore into users(user_id, screen_name) values (?, ?);',
                                      self.user_buf)

        file_count = 1
        file = None
        try:
            cur = self.conn.cursor()
            for user_count, row in enumerate(
                    cur.execute("select user_id, tweet_count from tweets order by tweet_count desc")):
                user_id = row[0]
                tweet_count = row[1]
                # Cycle tweet id files
                if user_count % self.max_per_file == 0:
                    if file:
                        file.close()
                    file = open(
                        os.path.join(self.dataset_path, 'top-users-{}.csv'.format(str(file_count).zfill(3))), 'w')
                    file_count += 1
                # Get screen names
                screen_names = []
                for user_row in self.conn.execute("select screen_name from users where user_id=?", (user_id,)):
                    screen_names.append(user_row[0])

                # Write to mentions to file
                line = [user_id, str(tweet_count)]
                line.extend(screen_names)
                file.write(','.join(line))
                file.write('\n')

                if (user_count + 1) % self.generate_update_increment == 0:
                    self.update_state(user_count + 1,
                                      self.total_user_count,
                                      '{:,d} of {:,d} users in {:,d} files'.format(user_count + 1,
                                                                                   self.total_user_count,
                                                                                   file_count))
        finally:
            if file:
                file.close()
        os.remove(self.db_filepath)


task_class_map = {
    'tweet_ids': GenerateTweetIdsTask,
    'tweet_json': GenerateTweetJSONTask,
    'tweet_csv': GenerateTweetCSVTask,
    'mentions': GenerateMentionsTask,
    'top_mentions': GenerateTopMentionsTask,
    'top_users': GenerateTopUsersTask
}


def _mention_iter(buf):
    for mention_user_id, count in buf.items():
        yield mention_user_id, count
