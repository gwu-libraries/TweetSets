import json
import uuid
from datetime import datetime
import re
from elasticsearch_dsl import Search, Q
from models import get_tweets_index_name


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


def dataset_params_to_search(dataset_params):
    indexes = []
    for source_dataset in dataset_params.get('source_datasets'):
        indexes.append(get_tweets_index_name(source_dataset))
    search = Search(index=indexes)

    # Query
    q = None
    # # Source datasets
    # if dataset_params.get('source_datasets'):
    #     q = _and(q, Q('terms', dataset_id=dataset_params.get('source_datasets')))
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
    search.aggs.bucket('top_users', 'terms', field='user_screen_name', size=10)
    search.aggs.bucket('top_hashtags', 'terms', field='hashtags', size=10)
    search.aggs.bucket('top_mentions', 'terms', field='mention_screen_names', size=10)
    search.aggs.bucket('top_urls', 'terms', field='urls', size=10)
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

