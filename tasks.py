import os
import fnmatch
import sqlite3
import gzip
import json
import csv
from utils import dataset_params_to_search
from dateutil.parser import parse as date_parse
import logging

logger = logging.getLogger(__name__)


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
            file.write(bytes(hit.meta.id, 'utf-8'))
            file.write(bytes('\n', 'utf-8'))
            if (tweet_count + 1) % generate_update_increment == 0:
                self.update_state(state='PROGRESS',
                                  meta={'current': tweet_count + 1, 'total': total_tweets,
                                        'status': '{:,d} of {:,d} tweet ids in {:,d} files'.format(tweet_count + 1,
                                                                                                   total_tweets,
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
                                        'status': '{:,d} of {:,d} tweets in {:,d} files'.format(tweet_count + 1,
                                                                                                total_tweets,
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
                    # Encountered instances where mention_user_id is null.
                    if mention_user_id:
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
                                        'status': '{:,d} of {:,d} tweets contains {:,d} mentions in {:,d} files'.format(
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
                # Encountered unexpected blank user ids
                if mention_user_id:
                    mention_count += 1
                    mention_screen_name = hit.mention_screen_names[i]

                    if mention_user_id in buf:
                        buf[mention_user_id][0] += 1
                    else:
                        cur = conn.cursor()
                        cur.execute('update mentions set mention_count=mention_count+1 where user_id=?',
                                    (mention_user_id,))
                        if not cur.rowcount:
                            buf[mention_user_id] = [1, mention_screen_name]
                        conn.commit()

                    if len(buf) and len(buf) % 1000 == 0:
                        with conn:
                            conn.executemany(
                                'insert into mentions(user_id, screen_name, mention_count) values (?, ?, ?);',
                                _mention_iter(buf))
                        total_user_count += len(buf)
                        buf = dict()

        if (tweet_count + 1) % generate_update_increment == 0:
            self.update_state(state='PROGRESS',
                              meta={'current': tweet_count + 1, 'total': total_tweets,
                                    'status': 'Counted {:,d} mentions in {:,d} of {:,d} tweets'.format(
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
                                        'status': '{:,d} of {:,d} mentioners in {:,d} files'.format(
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


def generate_tweet_csv_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                            generate_update_increment=None):
    max_per_file = max_per_file or 250000
    generate_update_increment = generate_update_increment or 10000
    search = dataset_params_to_search(dataset_params)
    search.source(['tweet'])

    # Delete existing files
    for filename in fnmatch.filter(os.listdir(dataset_path), "tweets-*.csv"):
        os.remove(os.path.join(dataset_path, filename))

    file_count = 1
    file = None
    tweet_count = 0
    writer = None
    try:
        for tweet_count, hit in enumerate(search.scan()):
            # This is to support limiting the number of tweets
            if tweet_count + 1 > total_tweets:
                break
            # Cycle tweet id files
            if tweet_count % max_per_file == 0:
                if file:
                    file.close()
                file = open(
                    os.path.join(dataset_path, 'tweets-{}.csv'.format(str(file_count).zfill(3))), 'w')
                writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(_csv_header_row())
                file_count += 1
            # Write to tweet file
            writer.writerow(_csv_row(hit.tweet.to_dict()))
            if (tweet_count + 1) % generate_update_increment == 0:
                self.update_state(state='PROGRESS',
                                  meta={'current': tweet_count + 1, 'total': total_tweets,
                                        'status': '{:,d} of {:,d} tweets in {:,d} files'.format(tweet_count + 1,
                                                                                                total_tweets,
                                                                                                file_count)})
    finally:
        if file:
            file.close()

    generate_task_filepath = os.path.join(dataset_path, 'generate_tweet_csv_task.json')
    if os.path.exists(generate_task_filepath):
        os.remove(generate_task_filepath)

    return {'current': tweet_count + 1, 'total': total_tweets,
            'status': 'Completed.'}


def _csv_header_row():
    return ['created_at', 'twitter_id', 'screen_name', 'location', 'followers_count',
            'friends_count', 'favorite_count/like_count', 'retweet_count',
            'hashtags', 'mentions', 'in_reply_to_screen_name',
            'twitter_url', 'text', 'is_retweet', 'is_quote', 'coordinates',
            'url1', 'url1_expanded', 'url2', 'url2_expanded', 'media_url']


def _csv_row(tweet):
    row = [date_parse(tweet['created_at']),
           tweet['id_str'],
           tweet['user']['screen_name'],
           tweet['user']['location'],
           tweet['user']['followers_count'],
           tweet['user']['friends_count'],
           tweet['favorite_count'],
           tweet['retweet_count'],
           ', '.join([hashtag['text'] for hashtag in tweet['entities']['hashtags']]),
           ', '.join([user_mentions['screen_name'] for user_mentions in tweet['entities']['user_mentions']]),
           tweet.get('in_reply_to_screen_name', ''),
           'http://twitter.com/{}/status/{}'.format(tweet['user']['screen_name'], tweet['id_str']),
           (tweet.get('full_text') or tweet.get('extended_tweet', {}).get('full_text') or tweet['text']).replace('\n',
                                                                                                                 ' '),
           'Yes' if 'retweeted_status' in tweet else 'No',
           'Yes' if 'quoted_status_id' in tweet else 'No',
           str(tweet['coordinates']['coordinates']) if tweet.get('coordinates') else ''
           ]
    # only show up to two urls w/expansions
    urlslist = []
    entities = tweet.get('extended_tweet', {}).get('entities') or tweet['entities']
    for url in entities['urls'][:2]:
        urlslist += [url['url'], url['expanded_url']]
    # Padding the row if URLs do not take up all 4 columns
    row += urlslist + [''] * (4 - len(urlslist))
    if 'media' in entities:
        # Only export the first media_url (haven't yet seen tweets with >1)
        for media in entities['media'][:1]:
            row += [media['media_url']]
    else:
        row += ['']
    return row
