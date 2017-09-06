import os
import fnmatch
import sqlite3
import gzip
import json
from utils import dataset_params_to_search


def generate_tweet_ids_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                            generate_update_increment=None):
    max_per_file = max_per_file or 1000000
    generate_update_increment = generate_update_increment or 10000
    search = dataset_params_to_search(dataset_params)

    # Delete existing files
    for filename in fnmatch.filter(os.listdir(dataset_path), "tweet-ids-*.txt.gz"):
        os.remove(os.path.join(dataset_path, filename))

    file_count = 1
    file = None
    tweet_count = 0
    try:
        for tweet_count, hit in enumerate(search.scan()):
            # This is to support limiting the number of tweets
            if tweet_count + 1 > total_tweets:
                break
            # Cycle tweet id files
            if tweet_count % max_per_file == 0:
                if file:
                    file.close()
                file = gzip.open(
                    os.path.join(dataset_path, 'tweet-ids-{}.txt.gz'.format(str(file_count).zfill(3))), 'wb')
                file_count += 1
            # Write to tweet id file
            file.write(bytes(hit.meta.id[7:], 'utf-8'))
            file.write(bytes('\n', 'utf-8'))
            if (tweet_count + 1) % generate_update_increment == 0:
                self.update_state(state='PROGRESS',
                                  meta={'current': tweet_count + 1, 'total': total_tweets,
                                        'status': '{} of {} tweet ids in {} files'.format(tweet_count + 1, total_tweets,
                                                                                          file_count)})
    finally:
        if file:
            file.close()

    generate_task_filepath = os.path.join(dataset_path, 'generate_tweet_ids_task.json')
    if os.path.exists(generate_task_filepath):
        os.remove(generate_task_filepath)

    return {'current': tweet_count + 1, 'total': total_tweets,
            'status': 'Completed.'}


def generate_tweet_json_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                             generate_update_increment=None):
    max_per_file = max_per_file or 1000000
    generate_update_increment = generate_update_increment or 10000
    search = dataset_params_to_search(dataset_params)
    search.source(['tweet'])

    # Delete existing files
    for filename in fnmatch.filter(os.listdir(dataset_path), "tweets-*.json.gz"):
        os.remove(os.path.join(dataset_path, filename))

    file_count = 1
    file = None
    tweet_count = 0
    try:
        for tweet_count, hit in enumerate(search.scan()):
            # This is to support limiting the number of tweets
            if tweet_count + 1 > total_tweets:
                break
            # Cycle tweet id files
            if tweet_count % max_per_file == 0:
                if file:
                    file.close()
                file = gzip.open(
                    os.path.join(dataset_path, 'tweets-{}.json.gz'.format(str(file_count).zfill(3))), 'wb')
                file_count += 1
            # Write to tweet file
            file.write(bytes(json.dumps(hit.tweet.to_dict()), 'utf-8'))
            file.write(bytes('\n', 'utf-8'))
            if (tweet_count + 1) % generate_update_increment == 0:
                self.update_state(state='PROGRESS',
                                  meta={'current': tweet_count + 1, 'total': total_tweets,
                                        'status': '{} of {} tweets in {} files'.format(tweet_count + 1, total_tweets,
                                                                                       file_count)})
    finally:
        if file:
            file.close()

    generate_task_filepath = os.path.join(dataset_path, 'generate_tweet_json_task.json')
    if os.path.exists(generate_task_filepath):
        os.remove(generate_task_filepath)

    return {'current': tweet_count + 1, 'total': total_tweets,
            'status': 'Completed.'}


def generate_mentions_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                           generate_update_increment=None):
    max_per_file = max_per_file or 1000000
    generate_update_increment = generate_update_increment or 10000

    search = dataset_params_to_search(dataset_params)
    search.source(['mention_user_ids', 'user_id'])

    # Delete existing files
    for filename in fnmatch.filter(os.listdir(dataset_path), "mention-edges-*.csv.gz"):
        os.remove(os.path.join(dataset_path, filename))

    # Create db
    db_filepath = os.path.join(dataset_path, "mentions.db")
    if os.path.exists(db_filepath):
        os.remove(db_filepath)
    conn = sqlite3.connect(db_filepath)
    with conn:
        conn.execute('create table user_ids (user_id primary key);')

    file_count = 1
    edges_file = None
    nodes_file = gzip.open(os.path.join(dataset_path, 'mention-nodes.csv.gz'), 'wb')
    mention_count = 0
    tweet_count = 0
    try:
        for tweet_count, hit in enumerate(search.scan()):
            # This is to support limiting the number of tweets
            if tweet_count + 1 > total_tweets:
                break
            # Cycle tweet id files
            if tweet_count % max_per_file == 0:
                if edges_file:
                    edges_file.close()
                edges_file = gzip.open(
                    os.path.join(dataset_path, 'mention-edges-{}.csv.gz'.format(str(file_count).zfill(3))), 'wb')
                file_count += 1
            # Write to mentions to file
            if hasattr(hit, 'mention_user_ids'):
                for i, mention_user_id in enumerate(hit.mention_user_ids):
                    mention_count += 1
                    # Write mention user id (edge)
                    edges_file.write(bytes(','.join([hit.user_id, mention_user_id]), 'utf-8'))
                    edges_file.write(bytes('\n', 'utf-8'))

                    # Possibly write mention user id to mention screen name (node)
                    try:
                        with conn:
                            conn.execute('insert into user_ids(user_id) values (?);', (mention_user_id,))
                        nodes_file.write(bytes(','.join([mention_user_id, hit.mention_screen_names[i]]), 'utf-8'))
                        nodes_file.write(bytes('\n', 'utf-8'))
                    except sqlite3.IntegrityError:
                        # A dupe, so skipping writing to nodes file
                        pass

            if (tweet_count + 1) % generate_update_increment == 0:
                self.update_state(state='PROGRESS',
                                  meta={'current': tweet_count + 1, 'total': total_tweets,
                                        'status': '{} of {} tweets contains {} mentions in {} files'.format(
                                            tweet_count + 1, total_tweets,
                                            mention_count, file_count)})
    finally:
        if edges_file:
            edges_file.close()
        nodes_file.close()

    generate_task_filepath = os.path.join(dataset_path, 'generate_mentions_task.json')
    if os.path.exists(generate_task_filepath):
        os.remove(generate_task_filepath)
    conn.close()
    os.remove(db_filepath)

    return {'current': tweet_count + 1, 'total': total_tweets,
            'status': 'Completed.'}


def generate_top_mentions_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                               generate_update_increment=None):
    max_per_file = max_per_file or 1000000
    generate_update_increment = generate_update_increment or 10000

    search = dataset_params_to_search(dataset_params)
    search.source(['mention_user_ids', 'user_id'])

    # Delete existing files
    for filename in fnmatch.filter(os.listdir(dataset_path), "top-mentions-*.csv.gz"):
        os.remove(os.path.join(dataset_path, filename))

    # Create db
    db_filepath = os.path.join(dataset_path, "top-mentions.db")
    if os.path.exists(db_filepath):
        os.remove(db_filepath)
    conn = sqlite3.connect(db_filepath)
    with conn:
        conn.execute('create table mentions(user_id primary key, screen_name text, mention_count int);')

    self.update_state(state='PROGRESS',
                      meta={'current': 0, 'total': 1,
                            'status': 'Querying'})

    mention_count = 0
    total_user_count = 0
    buf = dict()
    for tweet_count, hit in enumerate(search.scan()):
        # This is to support limiting the number of tweets
        if tweet_count + 1 > total_tweets:
            break
        if hasattr(hit, 'mention_user_ids'):
            for i, mention_user_id in enumerate(hit.mention_user_ids):
                mention_count += 1
                mention_screen_name = hit.mention_screen_names[i]

                if mention_user_id in buf:
                    buf[mention_user_id][0] += 1
                else:
                    cur = conn.cursor()
                    cur.execute('update mentions set mention_count=mention_count+1 where user_id=?', (mention_user_id,))
                    if not cur.rowcount:
                        buf[mention_user_id] = [1, mention_screen_name]
                    conn.commit()

                if len(buf) and len(buf) % 1000 == 0:
                    with conn:
                        conn.executemany('insert into mentions(user_id, screen_name, mention_count) values (?, ?, ?);',
                                         _mention_iter(buf))
                    total_user_count += len(buf)
                    buf = dict()

        if (tweet_count + 1) % generate_update_increment == 0:
            self.update_state(state='PROGRESS',
                              meta={'current': tweet_count + 1, 'total': total_tweets,
                                    'status': 'Counted {} mentions in {} of {} tweets'.format(
                                        mention_count, tweet_count + 1, total_tweets)})

    # Final write of buffer
    if len(buf):
        with conn:
            conn.executemany('insert into mentions(user_id, screen_name, mention_count) values (?, ?, ?);',
                             _mention_iter(buf))
        total_user_count += len(buf)

    file_count = 1
    file = None
    user_count = 0
    try:
        cur = conn.cursor()
        for user_count, row in enumerate(
                cur.execute("select user_id, screen_name, mention_count from mentions order by mention_count desc")):
            # Cycle tweet id files
            if user_count % max_per_file == 0:
                if file:
                    file.close()
                file = gzip.open(
                    os.path.join(dataset_path, 'top-mentions-{}.csv.gz'.format(str(file_count).zfill(3))), 'wb')
                file_count += 1
            # Write to mentions to file
            file.write(bytes(','.join([row[0], row[1], str(row[2])]), 'utf-8'))
            file.write(bytes('\n', 'utf-8'))

            if (user_count + 1) % generate_update_increment == 0:
                self.update_state(state='PROGRESS',
                                  meta={'current': user_count + 1, 'total': total_user_count,
                                        'status': '{} of {} mentioners in {} files'.format(
                                            user_count + 1, total_user_count, file_count)})
    finally:
        if file:
            file.close()

    generate_task_filepath = os.path.join(dataset_path, 'generate_top_mentions_task.json')
    if os.path.exists(generate_task_filepath):
        os.remove(generate_task_filepath)
    conn.close()
    os.remove(db_filepath)

    return {'current': user_count + 1, 'total': total_user_count,
            'status': 'Completed.'}


def _mention_iter(buf):
    for mention_user_id, (count, screen_name) in buf.items():
        yield mention_user_id, screen_name, count
