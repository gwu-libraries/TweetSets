from flask import Flask, render_template, request, send_file, jsonify, redirect, url_for, flash, make_response
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections as es_connections
import os
from celery import Celery
import gzip
import requests
import fnmatch
import re
from models import DatasetDocType
import sqlite3
import redis as redispy
from datetime import date, datetime, timedelta
import json
import ipaddress

from utils import read_json, write_json, short_uid

# Flask setup
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'secret')
app.config['DATASETS_PATH'] = '/tweetsets_data/datasets'
app.config['MAX_PER_FILE'] = os.environ.get('MAX_PER_FILE')
app.config['GENERATE_UPDATE_INCREMENT'] = os.environ.get('GENERATE_UPDATE_INCREMENT')
app.config['SERVER_MODE'] = os.environ.get('SERVER_MODE', 'local')
app.config['IP_RANGE'] = os.environ.get('IP_RANGE')

# ElasticSearch setup
es_connections.create_connection(hosts=['elasticsearch'], timeout=20)

# Celery setup
app.config['CELERY_BROKER_URL'] = 'redis://redis:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://redis:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

redis = redispy.StrictRedis(host='redis', port=6379, db=1, decode_responses=True)

# IP ranges
ip_ranges = []
if app.config['IP_RANGE']:
    for address in app.config['IP_RANGE'].split(','):
        ip_ranges.append(ipaddress.IPv4Network(address))


@app.route('/')
def about():
    return render_template('about.html',
                           tweet_count=_tweet_count(),
                           prev_datasets=json.loads(request.cookies.get('prev_datasets', '[]')),
                           is_local_mode=_is_local_mode(request))


@app.route('/datasets')
def dataset_list():
    search = DatasetDocType.search()
    search.sort('name')
    if not _is_local_mode(request):
        search = search.filter('term', local_only=False)

    return render_template('dataset_list.html',
                           server_mode=app.config['SERVER_MODE'],
                           datasets=search.execute(),
                           prev_datasets = json.loads(request.cookies.get('prev_datasets', '[]')))


@app.route('/dataset/<dataset_id>', methods=['GET', 'POST'])
def dataset(dataset_id):
    dataset_path = _dataset_path(dataset_id)

    # Read dataset_params
    dataset_params = read_json(os.path.join(dataset_path, 'dataset_params.json'))
    # Create context
    context = _prepare_dataset_view(dataset_params)

    # Generate tweet ids
    generate_tweet_ids_task_filepath = os.path.join(dataset_path, 'generate_tweet_ids_task.json')
    if request.form.get('generate_tweet_ids', '').lower() == 'true' and not os.path.exists(
            generate_tweet_ids_task_filepath):
        app.logger.info('Generating tweet ids for {}'.format(dataset_id))
        flash('Started generating tweet ids')
        generate_tweet_ids_task = _generate_tweet_ids_task.delay(dataset_params, context['total_tweets'], dataset_path,
                                                                 max_per_file=app.config[
                                                                     'MAX_PER_FILE'],
                                                                 generate_update_increment=app.config[
                                                                     'GENERATE_UPDATE_INCREMENT'])
        # Write task.json
        write_json(generate_tweet_ids_task_filepath, {'id': generate_tweet_ids_task.id})
        context['tweet_id_task_id'] = generate_tweet_ids_task.id
    elif os.path.exists(generate_tweet_ids_task_filepath):
        context['tweet_id_task_id'] = read_json(generate_tweet_ids_task_filepath)['id']
    else:
        # Check for existing derivatives
        context['tweet_id_filenames'] = fnmatch.filter(os.listdir(dataset_path), "tweet-ids-*.txt.gz")

    # Generate tweet json
    if context['is_local_mode']:
        generate_tweet_json_task_filepath = os.path.join(dataset_path, 'generate_tweet_json_task.json')
        if request.form.get('generate_tweet_json', '').lower() == 'true' and not os.path.exists(
                generate_tweet_json_task_filepath):
            app.logger.info('Generating tweet json for {}'.format(dataset_id))
            flash('Started generating tweet JSON')
            generate_tweet_json_task = _generate_tweet_json_task.delay(dataset_params, context['total_tweets'], dataset_path,
                                                                     max_per_file=app.config[
                                                                         'MAX_PER_FILE'],
                                                                     generate_update_increment=app.config[
                                                                         'GENERATE_UPDATE_INCREMENT'])
            # Write task.json
            write_json(generate_tweet_json_task_filepath, {'id': generate_tweet_json_task.id})
            context['tweet_json_task_id'] = generate_tweet_json_task.id
        elif os.path.exists(generate_tweet_json_task_filepath):
            context['tweet_json_task_id'] = read_json(generate_tweet_json_task_filepath)['id']
        else:
            # Check for existing derivatives
            context['tweet_json_filenames'] = fnmatch.filter(os.listdir(dataset_path), "tweets-*.json.gz")

    # Generate mentions
    generate_mentions_task_filepath = os.path.join(dataset_path, 'generate_mentions_task.json')
    if request.form.get('generate_mentions', '').lower() == 'true' and not os.path.exists(
            generate_mentions_task_filepath):
        app.logger.info('Generating mentions for {}'.format(dataset_id))
        flash('Started generating mention')
        generate_mentions_task = _generate_mentions_task.delay(dataset_params, context['total_tweets'], dataset_path,
                                                               max_per_file=app.config[
                                                                   'MAX_PER_FILE'],
                                                               generate_update_increment=app.config[
                                                                   'GENERATE_UPDATE_INCREMENT'])

        # Write task.json
        write_json(generate_mentions_task_filepath, {'id': generate_mentions_task.id})
        context['mentions_task_id'] = generate_mentions_task.id
    elif os.path.exists(generate_mentions_task_filepath):
        context['mentions_task_id'] = read_json(generate_mentions_task_filepath)['id']
    else:
        # Check for existing derivatives
        context['mentions_filenames'] = fnmatch.filter(os.listdir(dataset_path), "mention-*.csv.gz")

    # Generate top mentions
    generate_top_mentions_task_filepath = os.path.join(dataset_path, 'generate_top_mentions_task.json')
    if request.form.get('generate_top_mentions', '').lower() == 'true' and not os.path.exists(
            generate_top_mentions_task_filepath):
        app.logger.info('Generating top mentions for {}'.format(dataset_id))
        flash('Started generating top mention')
        # _generate_top_mentions_task(dataset_params, context['total_tweets'], dataset_path)

        generate_top_mentions_task = _generate_top_mentions_task.delay(dataset_params, context['total_tweets'],
                                                                       dataset_path,
                                                                       max_per_file=app.config[
                                                                           'MAX_PER_FILE'],
                                                                       generate_update_increment=app.config[
                                                                           'GENERATE_UPDATE_INCREMENT'])

        # Write task.json
        write_json(generate_top_mentions_task_filepath, {'id': generate_top_mentions_task.id})
        context['top_mentions_task_id'] = generate_top_mentions_task.id
    elif os.path.exists(generate_top_mentions_task_filepath):
        context['top_mentions_task_id'] = read_json(generate_top_mentions_task_filepath)['id']
    else:
        # Check for existing derivatives
        context['top_mentions_filenames'] = fnmatch.filter(os.listdir(dataset_path), "top-mentions-*.csv.gz")

    context['dataset_id'] = dataset_id
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
    if request.form.get('dataset_name'):
        dataset_name = request.form['dataset_name']
        dataset_id = short_uid(8, lambda uid: os.path.exists(_dataset_path(uid)))
        app.logger.info('Creating {} ({})'.format(dataset_name, dataset_id))

        # Create dataset path
        dataset_path = _dataset_path(dataset_id)
        os.makedirs(dataset_path)
        # Write dataset_params
        write_json(os.path.join(dataset_path, 'dataset_params.json'), dataset_params)
        flash('Created dataset')
        # Add to prev_datasets
        prev_datasets = json.loads(request.cookies.get('prev_datasets', '[]'))
        prev_datasets.insert(0, {'dataset_name': dataset_name,
                                 'dataset_id': dataset_id,
                                 'create_date': date.today().isoformat()})
        resp = make_response(redirect('{}#datasetDerivatives'.format(url_for('dataset', dataset_id=dataset_id)),
                                      code=303))
        resp.set_cookie('prev_datasets', json.dumps(prev_datasets), expires=datetime.now() + timedelta(days=365 * 5))
        return resp

    return render_template('dataset.html', **context)


@app.route('/dataset_file/<dataset_id>/<filename>')
def dataset_file(dataset_id, filename):
    filepath = os.path.join(_dataset_path(dataset_id), filename)
    return send_file(filepath, as_attachment=True, attachment_filename=filename)


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
    oembed_error = False
    for hit in search_response:
        tweet_id = hit.meta.id[7:]
        sample_tweet_ids.append(tweet_id)
        if not oembed_error:
            try:
                tweet_html = _oembed(tweet_id)
                if tweet_html:
                    sample_tweet_html.append(tweet_html)
            except OembedException:
                # Skip further Oembed attemts
                oembed_error = True

    context['sample_tweet_ids'] = sample_tweet_ids
    context['sample_tweet_html'] = sample_tweet_html
    context['top_users'] = search_response.aggregations.top_users.buckets
    context['top_mentions'] = search_response.aggregations.top_mentions.buckets
    context['top_hashtags'] = search_response.aggregations.top_hashtags.buckets
    context['top_urls'] = search_response.aggregations.top_urls.buckets
    context['tweet_types'] = search_response.aggregations.tweet_types.buckets
    context['created_at_min'] = datetime.utcfromtimestamp(
        search_response.aggregations.created_at_min.value / 1000.0) \
        if search_response.aggregations.created_at_min.value else None
    context['created_at_max'] = datetime.utcfromtimestamp(
        search_response.aggregations.created_at_max.value / 1000.0) \
        if search_response.aggregations.created_at_max.value else None
    source_datasets = DatasetDocType.mget(dataset_params['source_datasets'])
    context['source_datasets'] = source_datasets
    dataset_created_at_min = None
    dataset_created_at_max = None
    for dataset in source_datasets:
        dataset_created_at_min = min(dataset_created_at_min,
                                     dataset.first_tweet_created_at) \
            if dataset_created_at_min else dataset.first_tweet_created_at
        dataset_created_at_max = min(dataset_created_at_max,
                                     dataset.last_tweet_created_at) \
            if dataset_created_at_max else dataset.last_tweet_created_at
    context['dataset_created_at_min'] = dataset_created_at_min
    context['dataset_created_at_max'] = dataset_created_at_max

    # Previous datasets
    context['prev_datasets'] = json.loads(request.cookies.get('prev_datasets', '[]'))

    # Mode
    context['is_local_mode'] = _is_local_mode(request)
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
            any_q = _or(any_q, Q('prefix', urls=url_prefix))
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
    app.logger.info(dataset_params)
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


def _dataset_params_to_context(dataset_params):
    context = dict()
    for key, value in dataset_params.items():
        if key != 'dataset_name':
            context['limit_{}'.format(key)] = value
        else:
            context['dataset_name'] = value
    return context


def _form_to_dataset_params(form):
    dataset_params = dict()
    for key, value in form.items():
        if key.startswith('limit_') and key != 'limit_source_datasets':
            dataset_params[key[6:]] = value
    dataset_params['source_datasets'] = form.getlist('limit_source_datasets')
    if 'dataset_name' in form:
        dataset_params['dataset_name'] = form['dataset_name']
    return dataset_params


def _tweet_count():
    tweet_count_str = redis.get('tweet_count')
    if not tweet_count_str:
        search = Search(index='tweets')
        search.query = Q()
        search_response = search.execute()
        tweet_count = search_response.hits.total
        redis.set('tweet_count', tweet_count, ex=24 * 60 * 60)
    else:
        tweet_count = int(tweet_count_str)
    return tweet_count


@celery.task(bind=True)
def _generate_tweet_ids_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                             generate_update_increment=None):
    max_per_file = max_per_file or 1000000
    generate_update_increment = generate_update_increment or 10000
    search = _dataset_params_to_search(dataset_params)

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


@celery.task(bind=True)
def _generate_tweet_json_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                             generate_update_increment=None):
    max_per_file = max_per_file or 1000000
    generate_update_increment = generate_update_increment or 10000
    search = _dataset_params_to_search(dataset_params)
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


@celery.task(bind=True)
def _generate_mentions_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                            generate_update_increment=None):
    max_per_file = max_per_file or 1000000
    generate_update_increment = generate_update_increment or 10000

    search = _dataset_params_to_search(dataset_params)
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


@celery.task(bind=True)
def _generate_top_mentions_task(self, dataset_params, total_tweets, dataset_path, max_per_file=None,
                                generate_update_increment=None):
    max_per_file = max_per_file or 1000000
    generate_update_increment = generate_update_increment or 10000

    search = _dataset_params_to_search(dataset_params)
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
                app.logger.info(user_count)
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


def _oembed(tweet_id):
    """
    Returns the HTML snippet for embedding the tweet.

    Uses cache otherwise retrieves from Twitter API.

    Raises OembedException if problem retrieving from Twitter API.
    """
    tweet_html = redis.get(tweet_id)
    if tweet_html:
        return tweet_html
    try:
        r = requests.get('https://publish.twitter.com/oembed',
                         params={'url': 'https://twitter.com/_/status/{}'.format(tweet_id),
                                 'omit_script': 'true',
                                 'hide_media': 'false',
                                 'hide_thread': 'false'},
                         timeout=5)
        if r:
            tweet_html = r.json()['html']
            redis.set(tweet_id, tweet_html, ex=24 * 60 * 60)
            return tweet_html
    except requests.exceptions.ConnectionError:
        raise OembedException()
    return None


class OembedException(Exception):
    pass


def _is_local_mode(request):
    """
    Returns true if the user is in local mode. Otherwise, user is in public mode.
    """
    # If in debug, can be set with is_local query parameter
    if app.config['DEBUG'] and 'is_local' in request.args:
        return request.args.get('is_local', 'false').lower() == 'true'

    # Use configured server mode for local and public
    if app.config['SERVER_MODE'] == 'local':
        return True
    elif app.config['SERVER_MODE'] == 'public':
        return False

    # For both, use local if in configured IP ranges.
    ip_address = ipaddress.ip_address(_get_ipaddr(request))
    for ip_range in ip_ranges:
        if ip_address in ip_range:
            return True

    return False


def _get_ipaddr(request):
    """
    Return the ip address for the current request (or 127.0.0.1 if none found)
    based on the X-Forwarded-For headers.
    """
    if request.access_route:
        return request.access_route[0]
    else:
        return request.remote_addr or '127.0.0.1'


@app.template_filter('nf')
def number_format_filter(num):
    """
    A filter for formatting numbers with commas.
    """
    return '{:,}'.format(num)
