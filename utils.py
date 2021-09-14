import json
import uuid
from datetime import datetime
import re
from elasticsearch_dsl import Search, Q
from models import get_tweets_index_name
from celery import Celery
from itertools import groupby
import logging
import os
from collections import defaultdict

log = logging.getLogger(__name__)

def make_celery(app):
    '''As described in Flask documentation. Adds a Flask application context to the Celery worker thread.'''
    celery = Celery(
        app.name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery

def create_extract_path(dataset_id):
    '''Creates a path for the tweet JSON extracts, using the supplied dataset_id. 
    :param dataset_id: unique identifier for this dataset'''
    full_dataset_path = os.environ.get('PATH_TO_EXTRACTS')
    if not full_dataset_path:
        log.error('ENV missing: PATH_TO_EXTRACTS. JSON extracts not copied.')
        return 
    json_extract_dir = os.path.join(full_dataset_path, dataset_id, 'tweet-json')
    if not os.path.isdir(json_extract_dir):
        os.makedirs(json_extract_dir)
    return json_extract_dir

def extract_date(filepath):
    '''
    Extracts the date portion of an SFM filepath.
    :param filepath: the string representation of a path to a file whose name is the format XXXXXXXXXX-YYYYMMDDXXXXXXXX-XXXXX-XXXXXXXX.json. If the path does not contain a valid date, will raise an error
    '''
    try:
        filename = filepath.split('/')[-1]
        fileparts = filename.split('-')
        date = fileparts[1][:8]
    except IndexError:
        log.exception(f'Error parsing JSON file names by date.')
        raise
    try:
        datetime.strptime(date, '%Y%m%d')
    except ValueError:
        log.exception(f'Invalid date format in {filepath}')
        raise  
    return date

def groupby_date(files):
    '''Groups a list of files from SFM by date, where the date is embedded in the filename. See the pattern in extract_date above. Returns a dictionary mapping the date to a list of files.
    :param files: a list of file paths.'''
    sorted_files = sorted(files, key=extract_date)
    groups = {k: list(g) for k, g in groupby(sorted_files, extract_date)}
    return groups

def groupby_size(files, max_size):
    '''Groups a list of files up to the max_size parameter. Returns a dict mapping a group index to a list of files.
    :param files: a list of file paths
    :param max_size: maximum size in bytes'''
    groups = defaultdict(list) # 
    size = 0 # Counter for size
    i = 1 # Index for grouped file names
    for f in files:
        size += os.stat(f).st_size
        # If the size of this file would put us over the limit, and if the current group isn't empty, we create a new group
        if (size >= max_size) and (len(groups[str(i).zfill(5)]) > 0):
            size = os.stat(f).st_size
            i += 1
        groups[str(i).zfill(5)].append(f)
    return groups

def write_json(filepath, obj):
    with open(filepath, 'w') as file:
        json.dump(obj, file)


def read_json(filepath):
    with open(filepath) as file:
        obj = json.load(file)
    return obj


def short_uid(length, exists_func):
    while True:
        uid = uuid.uuid4().hex[0:length]
        if not exists_func(uid):
            return uid


def dataset_params_to_search(dataset_params, skip_aggs=False, max_aggs=1000):
    source_dataset = dataset_params.get('source_dataset')
    index = get_tweets_index_name(source_dataset)
    search = Search(index=index).extra(track_total_hits=True)

    # Query
    q = None

    # Text
    if dataset_params.get('tweet_text_all'):
        for term in re.split(', *', dataset_params['tweet_text_all']):
            if ' ' in term:
                q = _and(q, Q('match_phrase', text=term))
            else:
                q = _and(q, Q('match', text=term))
    if dataset_params.get('tweet_text_any'):
        any_q = None
        for term in re.split(', *', dataset_params['tweet_text_any']):
            if ' ' in term:
                any_q = _or(any_q, Q('match_phrase', text=term))
            else:
                any_q = _or(any_q, Q('match', text=term))
        q = _and(q, any_q)
    if dataset_params.get('tweet_text_exclude'):
        for term in re.split(', *', dataset_params['tweet_text_exclude']):
            if ' ' in term:
                q = _and(q, ~Q('match_phrase', text=term))
            else:
                q = _and(q, ~Q('match', text=term))

    # Hashtags
    if dataset_params.get('hashtag_any'):
        hashtags = []
        for hashtag in re.split(', *', dataset_params['hashtag_any']):
            hashtags.append(hashtag.lstrip('#').lower())
        if hashtags:
            q = _and(q, Q('terms', hashtags=hashtags))

    # Mentions
    if dataset_params.get('mention_any'):
        mentions = []
        for mention in re.split(', *', dataset_params['mention_any']):
            mentions.append(mention.lstrip('@'))
        if mentions:
            q = _and(q, Q('terms', mention_screen_names=mentions))

    # Posted by
    if dataset_params.get('poster_any'):
        screen_names = []
        for screen_name in re.split(', *', dataset_params['poster_any']):
            screen_names.append(screen_name.lstrip('@'))
        if screen_names:
            any_q = Q('terms', user_screen_name=screen_names)
            if dataset_params.get('poster_retweets_also', '').lower() == 'true':
                any_q = _or(any_q, Q('terms', retweeted_quoted_screen_name=screen_names))
            q = _and(q, any_q)
    if dataset_params.get('poster_user_id_any'):
        user_ids = []
        for user_id in re.split(', *', dataset_params['poster_user_id_any']):
            user_ids.append(user_id)
        if user_ids:
            any_q = Q('terms', user_id=user_ids)
            if dataset_params.get('poster_user_id_retweets_also', '').lower() == 'true':
                any_q = _or(any_q, Q('terms', retweeted_quoted_user_id=user_ids))
            q = _and(q, any_q)

    # Source posted by (i.e., the tweet that was retweeted or quoted)
    if dataset_params.get('source_poster_any'):
        screen_names = []
        for screen_name in re.split(', *', dataset_params['source_poster_any']):
            screen_names.append(screen_name.lstrip('@'))
        if screen_names:
            q = _and(q, Q('terms', retweeted_quoted_screen_name=screen_names))
    if dataset_params.get('source_poster_user_id_any'):
        user_ids = []
        for user_id in re.split(', *', dataset_params['source_poster_user_id_any']):
            user_ids.append(user_id)
        if user_ids:
            q = _and(q, Q('terms', retweeted_quoted_user_id=user_ids))

    # In reply to
    if dataset_params.get('in_reply_to_any'):
        screen_names = []
        for screen_name in re.split(', *', dataset_params['in_reply_to_any']):
            screen_names.append(screen_name.lstrip('@'))
        if screen_names:
            q = _and(q, Q('terms', in_reply_to_screen_name=screen_names))

    if dataset_params.get('in_reply_to_user_id_any'):
        user_ids = []
        for user_id in re.split(', *', dataset_params['in_reply_to_user_id_any']):
            user_ids.append(user_id)
        if user_ids:
            q = _and(q, Q('terms', in_reply_to_user_id=user_ids))

    # Tweet types
    tweet_types = []
    if dataset_params.get('tweet_type_original', '').lower() == 'true':
        tweet_types.append('original')
    if dataset_params.get('tweet_type_quote', '').lower() == 'true':
        tweet_types.append('quote')
    if dataset_params.get('tweet_type_retweet', '').lower() == 'true':
        tweet_types.append('retweet')
    if dataset_params.get('tweet_type_reply', '').lower() == 'true':
        tweet_types.append('reply')
    if len(tweet_types) != 4:
        q = _and(q, Q('terms', tweet_type=tweet_types))

    # Created at
    created_at_dict = {}
    if dataset_params.get('created_at_from'):
        created_at_dict['gte'] = datetime.strptime(dataset_params['created_at_from'], '%Y-%m-%d').date()
    if dataset_params.get('created_at_to'):
        created_at_dict['lte'] = datetime.strptime(dataset_params['created_at_to'], '%Y-%m-%d').date()
    if created_at_dict:
        q = _and(q, Q('range', created_at=created_at_dict))

    # Has media
    if dataset_params.get('has_media', '').lower() == 'true':
        q = _and(q, Q('term', has_media=True))

    # URL
    if dataset_params.get('has_url', '').lower() == 'true':
        q = _and(q, Q('exists', field='urls'))
    if dataset_params.get('url_any'):
        any_q = None
        for url_prefix in re.split(', *', dataset_params['url_any']):
            # Normalize to lower case and http
            any_q = _or(any_q, Q('prefix', urls=url_prefix.lower().replace('https://', 'http://')))
        q = _and(q, any_q)

    # Has geotag
    if dataset_params.get('has_geo', '').lower() == 'true':
        q = _and(q, Q('term', has_geo=True))

    search.query = Q('bool', filter=q or Q())

    # Aggregations
    if not skip_aggs:
        search.aggs.bucket('top_users', 'terms', field='user_screen_name', size=max_aggs)
        search.aggs.bucket('top_hashtags', 'terms', field='hashtags', size=max_aggs)
        search.aggs.bucket('top_mentions', 'terms', field='mention_screen_names', size=max_aggs)
        search.aggs.bucket('top_urls', 'terms', field='urls', size=max_aggs)
        search.aggs.bucket('tweet_types', 'terms', field='tweet_type')
        search.aggs.metric('created_at_min', 'min', field='created_at')
        search.aggs.metric('created_at_max', 'max', field='created_at')

    # Only get ids
    search.source(False)
    # Sort by _doc
    search.sort('_doc')
    return search


def _or(q1, q2):
    if q1 is None:
        return q2
    return q1 | q2


def _and(q1, q2):
    if q1 is None:
        return q2
    return q1 & q2
