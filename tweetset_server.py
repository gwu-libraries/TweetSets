from flask import Flask, render_template, request, send_file, jsonify, redirect, url_for, flash
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from elasticsearch_dsl.connections import connections as es_connections
import uuid
import os
from celery import Celery
import gzip
import json
from time import sleep
import requests
import fnmatch
from cachetools import LRUCache
import re
from models import DatasetDocType

# Flask setup
app = Flask(__name__)
app.config['SECRET_KEY'] = 'twitter'
app.config['DATASETS_PATH'] = '/tmp/dataset'
app.config['MAX_TWEET_IDS_PER_FILE'] = 1000
app.config['GENERATE_UPDATE_INCREMENT'] = 50
app.config['TWEET_HTML_QUEUE_SIZE'] = 500

# ElasticSearch setup
es_connections.create_connection(hosts=['localhost'], timeout=20)

# Celery setup
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

tweet_html_cache = LRUCache(maxsize=app.config['TWEET_HTML_QUEUE_SIZE'])


@app.route('/')
def dataset_list():
    return render_template('dataset_list.html', datasets = DatasetDocType.search())

@app.route('/dataset/<dataset_id>', methods=['GET'])
def dataset(dataset_id):
    dataset_path = _dataset_path(dataset_id)

    # Read dataset_params
    dataset_params = _read_json(os.path.join(dataset_path, 'dataset_params.json'))
    # Create context
    context = _prepare_dataset_view(dataset_params)

    generate_tweet_ids_task_filepath = os.path.join(dataset_path, 'generate_tweet_ids_task.json')
    if request.args.get('generate_tweet_ids', '').lower() == 'true':
        app.logger.info('Generating tweet ids for {}'.format(dataset_id))
        flash('Started generating tweet ids')
        generate_tweet_ids_task = _generate_tweet_ids_task.delay(dataset_params, context['total_tweets'], dataset_path,
                                                                 max_tweet_ids_per_file=app.config[
                                                                     'MAX_TWEET_IDS_PER_FILE'],
                                                                 generate_update_increment=app.config[
                                                                     'GENERATE_UPDATE_INCREMENT'])
        # Write task.json
        _write_json(generate_tweet_ids_task_filepath, {'id': generate_tweet_ids_task.id})
        context['task_id'] = generate_tweet_ids_task.id
    elif os.path.exists(generate_tweet_ids_task_filepath):
        context['task_id'] = _read_json(generate_tweet_ids_task_filepath)['id']
    else:
        # Check for existing derivatives
        context['tweet_id_filenames'] = fnmatch.filter(os.listdir(dataset_path), "tweet-ids-*.txt.gz")
    context['dataset_id'] = dataset_id
    app.logger.info(context.get('task_id'))
    return render_template('dataset.html', **context)


@app.route('/dataset', methods=['POST'])
def limit_dataset():
    dataset_params = _form_to_dataset_params(request.form)
    if list(dataset_params.keys()) == ['source_datasets']:
        dataset_params['tweet_type_original'] = 'true'
        dataset_params['tweet_type_retweet'] = 'true'
        dataset_params['tweet_type_quote'] = 'true'
        dataset_params['tweet_type_reply'] = 'true'
    context = _prepare_dataset_view(dataset_params)
    if request.form.get('create', '').lower() == 'true':
        dataset_id = uuid.uuid4().hex
        app.logger.info('Creating {}'.format(dataset_id))

        # Create dataset path
        dataset_path = _dataset_path(dataset_id)
        os.makedirs(dataset_path)
        # Write dataset_params
        _write_json(os.path.join(dataset_path, 'dataset_params.json'), dataset_params)
        flash('Created dataset')
        return redirect(url_for('dataset', dataset_id=dataset_id), code=303)

    return render_template('dataset.html', **context)


@app.route('/dataset_file/<dataset_id>/<filename>')
def dataset_file(dataset_id, filename):
    filepath = os.path.join(_dataset_path(dataset_id), filename)
    return send_file(filepath)


@app.route('/status/<task_id>')
def dataset_status(task_id):
    task = _generate_tweet_ids_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        # Task not started
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        # if 'result' in task.info:
        #     response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


def _prepare_dataset_view(dataset_params):
    context = _dataset_params_to_context(dataset_params)

    search = _dataset_params_to_search(dataset_params)
    search_response = search.execute()
    context['total_tweets'] = search_response.hits.total if not dataset_params.get('tweet_limit') else min(
        search_response.hits.total, int(dataset_params['tweet_limit']))
    sample_tweet_ids = []
    sample_tweet_html = []
    for hit in search_response:
        tweet_id = hit.meta.id[33:]
        sample_tweet_ids.append(tweet_id)
        tweet_html = _oembed(tweet_id)
        if tweet_html:
            sample_tweet_html.append(tweet_html)
    context['sample_tweet_ids'] = sample_tweet_ids
    context['sample_tweet_html'] = sample_tweet_html
    context['top_users'] = search_response.aggregations.top_users.buckets
    context['top_mentions'] = search_response.aggregations.top_mentions.buckets
    context['top_hashtags'] = search_response.aggregations.top_hashtags.buckets
    context['tweet_types'] = search_response.aggregations.tweet_types.buckets
    context['source_datasets'] = DatasetDocType.mget(dataset_params['source_datasets'])
    return context


def _dataset_path(dataset_id):
    return os.path.join(app.config['DATASETS_PATH'], dataset_id)


def _dataset_params_to_search(dataset_params):
    search = Search(index='tweets')

    # Query
    q = None
    # Source datasets
    if dataset_params.get('source_datasets'):
        q = _and(q, Q('terms', dataset_id=dataset_params.get('source_datasets')))
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

    search.query = Q('bool', filter=q or Q())

    # Aggregations
    search.aggs.bucket('top_users', 'terms', field='user_screen_name', size=10)
    search.aggs.bucket('top_hashtags', 'terms', field='hashtags', size=10)
    search.aggs.bucket('top_mentions', 'terms', field='mention_screen_names', size=10)
    search.aggs.bucket('tweet_types', 'terms', field='tweet_type')

    # Only get ids
    search.source(False)
    # Sort by _doc
    search.sort('_doc')
    app.logger.info(search.to_dict())
    return search


def _or(q1, q2):
    if q1 is None:
        return q2
    return q1 | q2


def _and(q1, q2):
    if q1 is None:
        return q2
    return q1 & q2


def _write_json(filepath, obj):
    with open(filepath, 'w') as file:
        json.dump(obj, file)


def _read_json(filepath):
    with open(filepath) as file:
        obj = json.load(file)
    return obj


def _dataset_params_to_context(dataset_params):
    context = dict()
    for key, value in dataset_params.items():
        context['limit_{}'.format(key)] = value
    return context


def _form_to_dataset_params(form):
    dataset_params = dict()
    for key, value in form.items():
        if key.startswith('limit_') and key != 'limit_source_datasets':
            dataset_params[key[6:]] = value
    dataset_params['source_datasets'] = form.getlist('limit_source_datasets')
    return dataset_params


@celery.task(bind=True)
def _generate_tweet_ids_task(self, dataset_params, total_tweets, dataset_path, max_tweet_ids_per_file=1000000,
                             generate_update_increment=10000):

    search = _dataset_params_to_search(dataset_params)
    file_count = 1
    tweet_id_file = None
    try:
        for tweet_count, hit in enumerate(search.scan()):
            if tweet_count + 1 > total_tweets:
                break
            if tweet_count % max_tweet_ids_per_file == 0:
                if tweet_id_file:
                    tweet_id_file.close()
                tweet_id_file = gzip.open(
                    os.path.join(dataset_path, 'tweet-ids-{}.txt.gz'.format(str(file_count).zfill(3))), 'wb')
                file_count += 1
            tweet_id_file.write(bytes(hit.meta.id[33:], 'utf-8'))
            tweet_id_file.write(bytes('\n', 'utf-8'))
            if (tweet_count + 1) % generate_update_increment == 0:
                self.update_state(state='PROGRESS',
                                  meta={'current': tweet_count + 1, 'total': total_tweets,
                                        'status': '{} of {} tweet ids in {} files'.format(tweet_count + 1, total_tweets,
                                                                                          file_count)})
    finally:
        if tweet_id_file:
            tweet_id_file.close()

    generate_tweet_ids_task_filepath = os.path.join(dataset_path, 'generate_tweet_ids_task.json')
    if os.path.exists(generate_tweet_ids_task_filepath):
        os.remove(generate_tweet_ids_task_filepath)

    return {'current': tweet_count + 1, 'total': total_tweets,
            'status': 'Completed.'}


def _oembed(tweet_id):
    tweet_html = tweet_html_cache.get(tweet_id)
    if tweet_html:
        return tweet_html
    try:
        r = requests.get('https://publish.twitter.com/oembed',
                         params={'url': 'https://twitter.com/_/status/{}'.format(tweet_id),
                                 'omit_script': 'true',
                                 'hide_media': 'false',
                                 'hide_thread': 'false'})
        if r:
            tweet_html = r.json()['html']
            tweet_html_cache[tweet_id] = tweet_html
            return tweet_html
    except requests.exceptions.ConnectionError:
        pass
    return None
