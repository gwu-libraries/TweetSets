from flask import Flask, render_template, request, send_file, jsonify, redirect, url_for, flash, make_response, \
    session, abort
from jinja2 import evalcontextfilter, Markup
from elasticsearch_dsl.connections import connections as es_connections
from elasticsearch.exceptions import ElasticsearchException
import os
from celery import Celery
import requests
import fnmatch
from models import DatasetDocType
import redis as redispy
from datetime import date, datetime, timedelta
import json
import ipaddress
from collections import namedtuple

from utils import read_json, write_json, short_uid, dataset_params_to_search
from stats import TweetSetStats
import tasks

# Flask setup
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'secret')
app.config['DATASETS_PATH'] = '/tweetsets_data/datasets'
app.config['MAX_PER_JSON_FILE'] = os.environ.get('MAX_PER_JSON_FILE', 10000000)
app.config['MAX_PER_CSV_FILE'] = os.environ.get('MAX_PER_CSV_FILE', 250000)
app.config['MAX_PER_TXT_FILE'] = os.environ.get('MAX_PER_TXT_FILE', 25000000)
app.config['GENERATE_UPDATE_INCREMENT'] = os.environ.get('GENERATE_UPDATE_INCREMENT')
app.config['SERVER_MODE'] = os.environ.get('SERVER_MODE', 'local')
app.config['IP_RANGE'] = os.environ.get('IP_RANGE')
app.config['HOST'] = os.environ.get('HOST') or 'localhost'
app.config['USE_TLS'] = os.environ.get('USE_TLS', 'false').lower() == 'true'
app.config['EMAIL_PORT'] = int(os.environ.get('EMAIL_PORT', '25'))
app.config['EMAIL_SMTP'] = os.environ.get('EMAIL_SMTP')
app.config['EMAIL_FROM'] = os.environ.get('EMAIL_FROM') or os.environ.get('EMAIL_USERNAME')
app.config['EMAIL_USERNAME'] = os.environ.get('EMAIL_USERNAME')
app.config['EMAIL_PASSWORD'] = os.environ.get('EMAIL_PASSWORD')
app.config['ADMIN_EMAIL'] = os.environ.get('ADMIN_EMAIL')
app.config['ES_TIMEOUT'] = int(os.environ.get('ES_TIMEOUT', '20'))

# ElasticSearch setup
es_connections.create_connection(hosts=['elasticsearch'], timeout=app.config['ES_TIMEOUT'], sniff_on_start=True,
                                 sniff_on_connection_fail=True, retry_on_timeout=True)
app.logger.debug('ElasticSearch timeout is %s', app.config['ES_TIMEOUT'])

# Celery setup
app.config['CELERY_BROKER_URL'] = 'redis://redis:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://redis:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

redis = redispy.StrictRedis(host='redis', port=6379, db=1, decode_responses=True)

ts_stats = TweetSetStats()

# IP ranges
ip_ranges = []
if app.config['IP_RANGE']:
    for address in app.config['IP_RANGE'].split(','):
        ip_ranges.append(ipaddress.IPv4Network(address))

# Email on error
if not app.debug and app.config['ADMIN_EMAIL'] and app.config['EMAIL_SMTP'] and app.config['EMAIL_FROM']:
    import logging
    from logging.handlers import SMTPHandler

    credentials = None
    if app.config['EMAIL_USERNAME'] and app.config['EMAIL_PASSWORD']:
        credentials = (app.config['EMAIL_USERNAME'], app.config['EMAIL_PASSWORD'])

    secure = None
    if app.config['USE_TLS']:
        secure = ()
    mail_handler = SMTPHandler((app.config['EMAIL_SMTP'], app.config['EMAIL_PORT']),
                               app.config['EMAIL_FROM'],
                               [app.config['ADMIN_EMAIL']], 'TweetSet error on {}'.format(app.config['HOST']),
                               credentials=('sfm_no_reply@email.gwu.edu', 'noreply4SFM!'), secure=secure)
    mail_handler.setLevel(logging.ERROR)
    app.logger.addHandler(mail_handler)


@app.route('/')
def about():
    # For testing
    if 'error' in request.args:
        raise Exception('Test exception')
    return render_template('about.html',
                           tweet_count=_tweet_count(clear_cache='clear_cache' in request.args),
                           prev_datasets=json.loads(request.cookies.get('prev_datasets', '[]')),
                           is_local_mode=_is_local_mode(request))


@app.route('/datasets')
def dataset_list():
    search = DatasetDocType.search().sort('name')[:1000]
    if not _is_local_mode(request):
        search = search.filter('term', local_only=False)

    dataset_list_msg = None
    if os.path.exists('dataset_list_msg.txt'):
        with open('dataset_list_msg.txt') as f:
            dataset_list_msg = f.read()

    return render_template('dataset_list.html',
                           dataset_list_msg=dataset_list_msg,
                           is_local_mode=_is_local_mode(request),
                           server_mode=app.config['SERVER_MODE'],
                           datasets=search.execute(),
                           prev_datasets=json.loads(request.cookies.get('prev_datasets', '[]')))


@app.route('/dataset/<dataset_id>', methods=['GET', 'POST'])
def dataset(dataset_id):
    dataset_path = _dataset_path(dataset_id)

    # Read dataset_params
    try:
        dataset_params = read_json(os.path.join(dataset_path, 'dataset_params.json'))
    except FileNotFoundError:
        abort(404)
    # Create context
    context = _prepare_dataset_view(dataset_params, clear_cache='clear_cache' in request.args)

    # Generate tasks
    generate_tasks_filepath = os.path.join(dataset_path, 'generate_tasks.json')
    if request.form.get('generate_tasks', '').lower() == 'true' and not os.path.exists(
            generate_tasks_filepath):
        app.logger.info('Generating task for {}'.format(dataset_id))
        task_defs = {}
        if request.form.get('generate_tweet_ids', '').lower() == 'true':
            task_defs['tweet_ids'] = {
                'max_per_file': app.config['MAX_PER_TXT_FILE']
            }
            # Record stats
            if not session.get("demo_mode", False):
                ts_stats.add_derivative('tweet ids', _is_local(request))
        if request.form.get('generate_tweet_json', '').lower() == 'true' and context['is_local_mode']:
            task_defs['tweet_json'] = {
                'max_per_file': app.config['MAX_PER_JSON_FILE']
            }
            # Record stats
            if not session.get("demo_mode", False):
                ts_stats.add_derivative('tweet json', _is_local(request))
        if request.form.get('generate_tweet_csv', '').lower() == 'true' and context['is_local_mode']:
            task_defs['tweet_csv'] = {
                'max_per_file': app.config['MAX_PER_CSV_FILE']
            }
            # Record stats
            if not session.get("demo_mode", False):
                ts_stats.add_derivative('tweet csv', _is_local(request))
        if request.form.get('generate_mentions', '').lower() == 'true':
            task_defs['mentions'] = {
                'max_per_file': app.config['MAX_PER_CSV_FILE']
            }
            # Record stats
            if not session.get("demo_mode", False):
                ts_stats.add_derivative('mentions', _is_local(request))

        if request.form.get('generate_top_mentions', '').lower() == 'true':
            task_defs['top_mentions'] = {
                'max_per_file': app.config['MAX_PER_CSV_FILE']
            }
            # Record stats
            if not session.get("demo_mode", False):
                ts_stats.add_derivative('top mentions', _is_local(request))

        if request.form.get('generate_top_users', '').lower() == 'true':
            task_defs['top_users'] = {
                'max_per_file': app.config['MAX_PER_CSV_FILE']
            }
            # Record stats
            if not session.get("demo_mode", False):
                ts_stats.add_derivative('top users', _is_local(request))

        if task_defs:
            generate_tasks = _generate_tasks.delay(task_defs, dataset_params, context['total_tweets'], dataset_path,
                                                   generate_update_increment=app.config[
                                                       'GENERATE_UPDATE_INCREMENT'])
            flash('Started generating derivatives')
            # Write task.json
            write_json(generate_tasks_filepath, {'id': generate_tasks.id})
            context['task_id'] = generate_tasks.id

    elif os.path.exists(generate_tasks_filepath):
        task_id = read_json(generate_tasks_filepath)['id']
        # Make sure task didn't fail
        task = _generate_tasks.AsyncResult(task_id)
        if task.state == 'FAILURE':
            os.remove(generate_tasks_filepath)
            flash('An error occurred generating derivates. Try again or <a href="mailto:{}">let me know</a>.'.format(
                app.config['ADMIN_EMAIL']), 'warning')
        else:
            context['task_id'] = task_id

    # Check for existing derivatives
    filenames_list = []
    _add_filenames('Generated tweet JSON files', 'tweets-*.jsonl.zip', dataset_path, filenames_list)
    _add_filenames('Generated tweet CSV files', 'tweets-*.csv.zip', dataset_path, filenames_list)
    _add_filenames('Generated tweet id files', 'tweet-ids-*.txt.zip', dataset_path, filenames_list)
    _add_filenames('Generated mentions files', 'mention-*.csv.zip', dataset_path, filenames_list)
    _add_filenames('Generated top mentions files', 'top-mentions-*.csv.zip', dataset_path, filenames_list)
    _add_filenames('Generated top users files', 'top-users-*.csv.zip', dataset_path, filenames_list)
    context['filenames_list'] = filenames_list

    context['dataset_id'] = dataset_id
    return render_template('dataset.html', **context)


def _add_filenames(label, filter, dataset_path, filename_list):
    filenames = fnmatch.filter(os.listdir(dataset_path), filter)
    if filenames:
        filenames.sort()
        filename_list.append((label, filenames))


@app.route('/dataset', methods=['POST'])
def limit_dataset():
    dataset_params = _form_to_dataset_params(request.form)
    if list(dataset_params.keys()) == ['source_datasets']:
        dataset_params['tweet_type_original'] = 'true'
        dataset_params['tweet_type_retweet'] = 'true'
        dataset_params['tweet_type_quote'] = 'true'
        dataset_params['tweet_type_reply'] = 'true'
    context = _prepare_dataset_view(dataset_params, clear_cache='clear_cache' in request.args)
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

        # Record stats
        if not session.get("demo_mode", False):
            is_local = _is_local(request)
            ts_stats.add_dataset(is_local, context['total_tweets'])
            for dataset_id in context['source_datasets']:
                app.logger.info(dataset_id)
                ts_stats.add_source_dataset(dataset_id.meta.id, is_local)
        return resp

    return render_template('dataset.html', **context)


@app.route('/dataset_file/<dataset_id>/<filename>')
def dataset_file(dataset_id, filename):
    filepath = os.path.join(_dataset_path(dataset_id), filename)
    return send_file(filepath, as_attachment=True, attachment_filename=filename)


@app.route('/status/<task_id>')
def dataset_status(task_id):
    task = _generate_tasks.AsyncResult(task_id)
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
            'status': 'Ooops! Something went wrong: {}'.format(str(task.info)),  # this is the exception raised
        }
        app.logger.error('Error with task id {}: {}'.format(task_id, str(task.info)))

    return jsonify(response)


@app.route('/stats')
def stats():
    # Handle demo mode
    if 'demo_mode' in request.args:
        if request.args['demo_mode'].lower() == 'true':
            session['demo_mode'] = True
        else:
            session['demo_mode'] = False

    since = datetime.utcnow() - timedelta(days=30 * 6)
    source_dataset_stats = ts_stats.source_datasets_merge_stats(since_datetime=since)
    source_dataset_names = {}
    # Get the names of the datasets.
    if source_dataset_stats:
        for source_dataset in DatasetDocType.mget([stat.dataset_id for stat in source_dataset_stats]):
            if source_dataset:
                source_dataset_names[source_dataset.meta.id] = source_dataset.name
    return render_template('stats.html',
                           all_datasets_stat=ts_stats.datasets_stats(),
                           local_datasets_stat=ts_stats.datasets_stats(local_only=True),
                           all_recent_datasets_stats=ts_stats.datasets_stats(since_datetime=since),
                           local_recent_dataset_stats=ts_stats.datasets_stats(since_datetime=since, local_only=True),
                           source_dataset_stats=source_dataset_stats,
                           source_dataset_names=source_dataset_names,
                           derivatives_stats=ts_stats.derivatives_merge_stats(since_datetime=since))


@app.route('/help')
def help():
    return render_template('help.html')


Node = namedtuple('Node', ['name', 'total_storage', 'available_storage', 'storage_status'])


@app.route('/healthcheck')
def healthcheck():
    # Return 200 if all green, 503 if any yellow, 500 if any red.
    cluster_status = 'red'
    nodes = []
    statuses = []
    try:
        r = requests.get('http://elasticsearch:9200/_cluster/stats', timeout=10)
        if r:
            stats = r.json()
            cluster_status = stats['status']
            statuses.append(cluster_status)
        r = requests.get('http://elasticsearch:9200/_nodes/stats/fs', timeout=10)
        if r:
            stats = r.json()
            for _, node_stats in stats['nodes'].items():
                node_name = node_stats['name']
                total_storage = node_stats['fs']['total']['total_in_bytes']
                available_storage = node_stats['fs']['total']['available_in_bytes']
                available = available_storage / total_storage
                storage_status = 'green'
                if .05 < available <= .2:
                    storage_status = 'yellow'
                elif available <= .05:
                    storage_status = 'red'
                statuses.append(storage_status)
                nodes.append(Node(node_name, total_storage, available_storage, storage_status))
    except requests.exceptions.ConnectionError:
        pass
    response_code = 200
    if 'yellow' in statuses:
        response_code = 503
    if 'red' in statuses:
        response_code = 500
    return render_template('healthcheck.html',
                           cluster_status=cluster_status,
                           nodes=nodes), response_code


@app.errorhandler(ElasticsearchException)
def handle_bad_request(e):
    return render_template('es_error.html')


def _prepare_dataset_view(dataset_params, clear_cache=False):
    context = _dataset_params_to_context(dataset_params)
    tweet_limit = 0
    try:
        tweet_limit = int(dataset_params.get('tweet_limit', '0') or '0')
    except ValueError:
        pass

    search_context = _search_to_search_context(dataset_params_to_search(dataset_params), dataset_params,
                                               tweet_limit=tweet_limit,
                                               clear_cache=clear_cache)
    context.update(search_context)
    context['sample_tweet_html'] = []
    oembed_error = False
    for tweet_id in context['sample_tweet_ids']:
        if not oembed_error:
            try:
                tweet_html = _oembed(tweet_id, clear_cache=clear_cache)
                if tweet_html:
                    context['sample_tweet_html'].append(tweet_html)
            except OembedException:
                # Skip further Oembed attemts
                oembed_error = True
    source_datasets = DatasetDocType.mget(dataset_params['source_datasets'])
    context['source_datasets'] = source_datasets
    dataset_created_at_min = None
    dataset_created_at_max = None
    for dataset in source_datasets:
        if dataset.first_tweet_created_at:
            if dataset_created_at_min:
                dataset_created_at_min = min(dataset_created_at_min, dataset.first_tweet_created_at)
            else:
                dataset_created_at_min = dataset.first_tweet_created_at
        if dataset.last_tweet_created_at:
            if dataset_created_at_max:
                dataset_created_at_max = max(dataset_created_at_max, dataset.last_tweet_created_at)
            else:
                dataset_created_at_max = dataset.last_tweet_created_at
    context['dataset_created_at_min'] = dataset_created_at_min
    context['dataset_created_at_max'] = dataset_created_at_max

    # Previous datasets
    context['prev_datasets'] = json.loads(request.cookies.get('prev_datasets', '[]'))

    # Mode
    context['is_local_mode'] = _is_local_mode(request)
    return context


def _search_to_search_context(search, dataset_params, tweet_limit=None, clear_cache=False):
    cache_context = redis.get(dataset_params)
    context = dict()
    if not cache_context or clear_cache:
        search_response = search.execute()
        context['total_tweets'] = search_response.hits.total if not tweet_limit else min(
            search_response.hits.total, tweet_limit)
        context['top_users'] = _buckets_to_list(search_response.aggregations.top_users.buckets)
        context['top_mentions'] = _buckets_to_list(search_response.aggregations.top_mentions.buckets)
        context['top_hashtags'] = _buckets_to_list(search_response.aggregations.top_hashtags.buckets)
        context['top_urls'] = _buckets_to_list(search_response.aggregations.top_urls.buckets)
        context['tweet_types'] = _buckets_to_list(search_response.aggregations.tweet_types.buckets)
        context['created_at_min_value'] = search_response.aggregations.created_at_min.value
        context['created_at_max_value'] = search_response.aggregations.created_at_max.value
        context['sample_tweet_ids'] = []
        for hit in search_response:
            tweet_id = hit.meta.id
            context['sample_tweet_ids'].append(tweet_id)
        redis.set(dataset_params, json.dumps(context), ex=24 * 60 * 60)
    else:
        context = json.loads(cache_context)
    context['created_at_min'] = datetime.utcfromtimestamp(
        context['created_at_min_value'] / 1000.0) \
        if context['created_at_min_value'] else None
    context['created_at_max'] = datetime.utcfromtimestamp(
        context['created_at_max_value'] / 1000.0) \
        if context['created_at_max_value'] else None

    return context


def _buckets_to_list(buckets):
    bucket_list = []
    for bucket in buckets:
        bucket_list.append({
            'key': bucket.key,
            'doc_count': bucket.doc_count
        })
    return bucket_list


def _dataset_path(dataset_id):
    return os.path.join(app.config['DATASETS_PATH'], dataset_id)


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


def _tweet_count(clear_cache=False):
    tweet_count_str = redis.get('tweet_count')
    if not tweet_count_str or clear_cache:
        tweet_count = 0
        search = DatasetDocType.search()
        for dataset in search.scan():
            tweet_count += (dataset.tweet_count or 0)
        app.logger.info('Counted %s tweets', tweet_count)
        redis.set('tweet_count', tweet_count, ex=24 * 60 * 60)
    else:
        tweet_count = int(tweet_count_str)
    return tweet_count


def _oembed(tweet_id, clear_cache=False):
    """
    Returns the HTML snippet for embedding the tweet.

    Uses cache otherwise retrieves from Twitter API.

    Raises OembedException if problem retrieving from Twitter API.
    """
    tweet_html = redis.get(tweet_id)
    if tweet_html and not clear_cache:
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


def _is_local_mode(req):
    """
    Returns true if the user is in local mode. Otherwise, user is in public mode.
    """
    # If in debug, can be set with is_local query parameter
    if app.config['DEBUG'] and 'is_local' in req.args:
        return req.args.get('is_local', 'false').lower() == 'true'

    # Use configured server mode for local and public
    if app.config['SERVER_MODE'] == 'local':
        return True
    elif app.config['SERVER_MODE'] == 'public':
        return False

    return _is_local(req)


def _is_local(req):
    """
    Returns true if user is in configured IP ranges.
    """
    ip_address = ipaddress.ip_address(_get_ipaddr(req))
    for ip_range in ip_ranges:
        if ip_address in ip_range:
            return True


def _get_ipaddr(req):
    """
    Return the ip address for the current request (or 127.0.0.1 if none found)
    based on the X-Forwarded-For headers.
    """
    if req.access_route:
        return req.access_route[0]
    else:
        return req.remote_addr or '127.0.0.1'


@app.template_filter('nf')
def number_format_filter(num):
    """
    A filter for formatting numbers with commas.
    """
    return '{:,}'.format(num) if num else 0


@app.template_filter('status')
@evalcontextfilter
def status_filter(eval_ctx, status):
    """
    Format a status.
    """
    result = None
    if status:
        if status.lower() == 'green':
            result = '<p class="text-success">{}</p>'.format(status)
        if status.lower() == 'yellow':
            result = '<p class="text-warning">{}</p>'.format(status)
        if status.lower() == 'red':
            result = '<p class="text-danger">{}</p>'.format(status)
    if result:
        if eval_ctx.autoescape:
            return Markup(result)
        else:
            return result
    return status


# Task
@celery.task(bind=True)
def _generate_tasks(self, task_defs, dataset_params, total_tweets, dataset_path, generate_update_increment=None):
    return tasks.generate_tasks(self, task_defs, dataset_params, total_tweets, dataset_path,
                                generate_update_increment=generate_update_increment)
