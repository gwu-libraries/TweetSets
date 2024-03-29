import click
from flask import Flask, render_template, request, send_file, jsonify, redirect, url_for, flash, make_response, \
    session, abort
from jinja2 import evalcontextfilter, Markup
from elasticsearch_dsl.connections import connections as es_connections
from elasticsearch.exceptions import ElasticsearchException
import os
import requests
from pathlib import Path
from models import DatasetDocument
import redis as redispy
from datetime import date, datetime, timedelta
import json
import ipaddress
from collections import namedtuple
from utils import read_json, write_json, short_uid, dataset_params_to_search, make_celery
from stats import TweetSetStats
import tasks

# Flask setup
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'secret')
app.config['DATASETS_PATH'] = '/tweetsets_data/datasets'
app.config['FULL_DATASETS_PATH'] = '/tweetsets_data/full_datasets'
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
app.config['CONSENT_BUTTON_TEXT'] = os.environ.get('CONSENT_BUTTON_TEXT')
app.config['CONSENT_HTML'] = os.environ.get('CONSENT_HTML')
app.config['GOOGLE_TAG'] = os.environ.get('GOOGLE_TAG')
# Maximum rows to display on the datasets stats page per statistic
app.config['MAX_TOP_ROWS_DS_STATS'] = 10
# Flask-Mail configs
app.config['MAIL_SERVER'] = app.config['EMAIL_SMTP']
app.config['MAIL_PORT'] = app.config['EMAIL_PORT']
app.config['MAIL_USERNAME'] = app.config['EMAIL_USERNAME']
app.config['MAIL_PASSWORD'] = app.config['EMAIL_PASSWORD']
app.config['MAIL_USE_TLS'] = app.config['USE_TLS']

# ElasticSearch setup
es_connections.create_connection(hosts=['elasticsearch'], timeout=app.config['ES_TIMEOUT'], sniff_on_start=True,
                                 sniff_on_connection_fail=True, retry_on_timeout=True)
app.logger.debug('ElasticSearch timeout is %s', app.config['ES_TIMEOUT'])

# Celery setup
app.config['CELERY_BROKER_URL'] = 'redis://redis:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://redis:6379/0'

celery = make_celery(app)

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
                               credentials=('sfm_no_reply@email.gwu.edu', app.config['EMAIL_PASSWORD']), secure=secure)
    mail_handler.setLevel(logging.ERROR)
    app.logger.addHandler(mail_handler)


@app.context_processor
def base_vars():
    consent_html = app.config['CONSENT_HTML']
    consent_button_text = app.config['CONSENT_BUTTON_TEXT']
    google_tag = app.config['GOOGLE_TAG']
    return dict(consent_html=consent_html, consent_button_text=consent_button_text, google_tag=google_tag)


@app.route('/')
def about():
    # For testing
    if 'error' in request.args:
        raise Exception('Test exception')
    return render_template('about.html',
                           tweet_count=_tweet_count(clear_cache='clear_cache' in request.args),
                           prev_datasets=json.loads(request.cookies.get('prev_datasets', '[]')),
                           is_local_mode=_is_local_mode(request),
                           )


@app.route('/datasets')
def dataset_list():
    search = DatasetDocument.search().sort('name')[:1000]
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
                           prev_datasets=json.loads(request.cookies.get('prev_datasets', '[]')),
                           )

@app.route('/full-dataset/<dataset_id>', methods=['GET', 'POST'], defaults={'full_dataset': True}, endpoint='full_dataset')
@app.route('/dataset/<dataset_id>', methods=['GET', 'POST'])
def dataset(dataset_id, full_dataset=False):
    dataset_path = _dataset_path(dataset_id, full_dataset=full_dataset)
    # Read dataset_params
    try:
        dataset_params = read_json(os.path.join(dataset_path, 'dataset_params.json'))
    except FileNotFoundError:
        abort(404)
    # Create context
    context = _prepare_dataset_view(dataset_params, clear_cache='clear_cache' in request.args,
                                    full_dataset=full_dataset)
    # Add requester email to dataset_params
    dataset_params['requester_email'] = request.form.get('requester_email', '')
    # Generate tasks
    generate_tasks_filepath = os.path.join(dataset_path, 'generate_tasks.json')
    if request.form.get('generate_tasks', '').lower() == 'true' and not os.path.exists(
            generate_tasks_filepath):
        app.logger.info('Generating task for {}'.format(dataset_id))
        task_defs = {}
        # Include email for notification
        task_defs['requester_email'] = dataset_params['requester_email']
        # Include URL to dataset
        task_defs['dataset_url'] = '{}#datasetExports'.format(request.base_url)
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
        if task_defs:
            generate_tasks = _generate_tasks.delay(task_defs, dataset_params, context['total_tweets'], dataset_path,
                                                   generate_update_increment=app.config[
                                                       'GENERATE_UPDATE_INCREMENT'])
            flash('Started generating exports')
            # Write task.json
            write_json(generate_tasks_filepath, {'id': generate_tasks.id})
            context['task_id'] = generate_tasks.id

    elif os.path.exists(generate_tasks_filepath):
        task_id = read_json(generate_tasks_filepath)['id']
        # Make sure task didn't fail
        task = _generate_tasks.AsyncResult(task_id)
        if task.state == 'FAILURE':
            os.remove(generate_tasks_filepath)
            flash('An error occurred generating exports. Try again or <a href="mailto:{}">let me know</a>.'.format(
                app.config['ADMIN_EMAIL']), 'warning')
        else:
            context['task_id'] = task_id

    # Check for existing exports
    filenames_list = []
    _add_filenames('Tweet JSON files', 'json', dataset_path, filenames_list, hide=not context['is_local_mode'])
    _add_filenames('Tweet CSV files', 'csv', dataset_path, filenames_list, hide=not context['is_local_mode'])
    _add_filenames('Tweet id files', 'ids', dataset_path, filenames_list)
    _add_filenames('Mentions nodes/edges files', 'nodes/edges', dataset_path, filenames_list)
    _add_filenames('Mention counts by user', 'mentions_agg', dataset_path, filenames_list)
    _add_filenames('Tweet counts by user', 'users_agg', dataset_path, filenames_list)
    context['filenames_list'] = filenames_list

    context['dataset_id'] = dataset_id
    # Save the user's emails to the dataset params file (if supplied)
    if dataset_params['requester_email']:
        write_json(os.path.join(dataset_path, 'dataset_params.json'), dataset_params)
    return render_template('dataset.html', **context)


def _add_filenames(label, filter, dataset_path, filename_list, hide=False):
    '''Adds files that match a given pattern within a supplied directory.
    :param label: a label to associate with this group of files
    :param filter: a type of file to search for; should correspond to a key in the patterns dict below
    :param dataset_path: a path where the files can be found
    :param filename_list: a list to be modified and returned, containing the files that match filter appended
    :param hide: whether to hide these files from this user
    '''
    patterns = {'json': ('tweets-*.jsonl.zip',
                        'tweet-json/*.json.gz',
                        'tweet-json/*.jsonl.gz',
                        'tweet-json/*.jsonl',
                        'tweet-json/*.json'),
                'ids': ('tweet-ids-*.txt.zip',
                        'tweet-ids/*.csv.gz'),
                'csv': ('tweets-*.csv.zip',
                        'tweet-csv/*.csv.gz'),
                'nodes/edges': ('mention-*.csv.zip',
                        'tweet-mentions/edges/*.csv.gz',
                        'tweet-mentions/nodes/*.csv.gz'),
                'mentions_agg': ('top-mentions-*.csv.zip',
                                'tweet-mentions/agg-mentions/*.csv.gz'),
                'users_agg': ('top-users-*.csv.zip',
                            'tweet-users/*.csv.gz')}
    p = Path(dataset_path)
    filenames = []
    # Iterate over possible patterns
    for pattern in patterns[filter]:
        # to account for nested directories
        pattern_path = Path(pattern)
        # Extract just the file name and add its relative parent
        filenames.extend([pattern_path.parent / f.name for f in  p.glob(pattern)])
    if filenames and not hide:
        filenames.sort()
        filename_list.append((label, filenames))

@app.route('/dataset', methods=['POST'])
def limit_dataset():
    dataset_params = _form_to_dataset_params(request.form)
    # Create context for template 
    # Need to add these values if we're coming from the /datasets route in order for the cache to get set correctly
    if set(dataset_params.keys()) == {'source_dataset'}:
        dataset_params.update({'tweet_type_{}'.format(key): 'true' 
                        for key in ('original', 'quote', 'retweet', 'reply')})
    context = _prepare_dataset_view(dataset_params, clear_cache='clear_cache' in request.args)
    # If params contains a name, assume user-defined dataset needs to be created
    if dataset_params.get('dataset_name'): 
        dataset_name = dataset_params['dataset_name']
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
        # Redirect response
        resp = make_response(redirect('{}#datasetExports'.format(url_for('dataset', dataset_id=dataset_id)),
                                      code=303))
        # Add cookies
        resp.set_cookie('prev_datasets', json.dumps(prev_datasets), expires=datetime.now() + timedelta(days=365 * 5))
        # Record stats
        if not session.get("demo_mode", False):
            is_local = _is_local(request)
            ts_stats.add_dataset(is_local, context['total_tweets'])
            dataset_id = context['source_dataset']
            ts_stats.add_source_dataset(dataset_id.meta.id, is_local)
        return resp
    elif dataset_params.get('is_full_dataset'):
        # Dataset ID is just the UUID for the source datasets
        dataset_id = dataset_params['source_dataset']
        dataset_path = _dataset_path(dataset_id, full_dataset=True)
        # Does the path exist? If not, generate extract
        if not os.path.exists(dataset_path):
            _create_extract(dataset_id)
        # Redirect to full_dataset route
        return redirect('{}#datasetExports'.format(url_for('full_dataset', dataset_id=dataset_id)),code=303)
    else:
        return render_template('dataset.html', **context)

@app.route('/full-dataset-file/<dataset_id>/<filename>', defaults={'full_dataset': True}, endpoint='full_dataset_file')
@app.route('/full-dataset-file/<dataset_id>/<path:extract_subpath>/<filename>', defaults={'full_dataset': True}, endpoint='full_dataset_file')
@app.route('/dataset-file/<dataset_id>/<filename>')
def dataset_file(dataset_id, filename, extract_subpath=None, full_dataset=False):
    if extract_subpath:
        filepath = os.path.join(_dataset_path(dataset_id, full_dataset), extract_subpath, filename)
    else:
        filepath = os.path.join(_dataset_path(dataset_id, full_dataset), filename)
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
    source_dataset_stats = ts_stats.source_datasets_stats(since_datetime=since)
    source_dataset_names = {}
    # Get the names of the datasets.
    if source_dataset_stats:
        for source_dataset in DatasetDocument.mget([stat.dataset_id for stat in source_dataset_stats]):
            if source_dataset:
                source_dataset_names[source_dataset.meta.id] = source_dataset.name
    return render_template('stats.html',
                           all_datasets_stat=ts_stats.datasets_stats(),
                           local_datasets_stat=ts_stats.datasets_stats(local_only=True),
                           all_recent_datasets_stats=ts_stats.datasets_stats(since_datetime=since),
                           local_recent_dataset_stats=ts_stats.datasets_stats(since_datetime=since, local_only=True),
                           source_dataset_stats=source_dataset_stats,
                           source_dataset_names=source_dataset_names,
                           derivatives_stats=ts_stats.derivatives_stats(since_datetime=since)
                           )


@app.route('/help')
def help():
    return render_template('help.html')


@app.route('/citing')
def citing():
    return render_template('citing.html')


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
            cluster_status = stats.get('status', 'unknown')
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


def _prepare_dataset_view(dataset_params, clear_cache=False, full_dataset=False):
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
    context['max_rows'] = app.config['MAX_TOP_ROWS_DS_STATS']
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
    source_dataset = DatasetDocument.get(dataset_params['source_dataset'])
    context['source_dataset'] = source_dataset
    dataset_created_at_min = None
    dataset_created_at_max = None
    if source_dataset.first_tweet_created_at:
        if dataset_created_at_min:
            dataset_created_at_min = min(dataset_created_at_min, source_dataset.first_tweet_created_at)
        else:
            dataset_created_at_min = source_dataset.first_tweet_created_at
    if source_dataset.last_tweet_created_at:
        if dataset_created_at_max:
            dataset_created_at_max = max(dataset_created_at_max, source_dataset.last_tweet_created_at)
        else:
            dataset_created_at_max = source_dataset.last_tweet_created_at
    context['dataset_created_at_min'] = dataset_created_at_min
    context['dataset_created_at_max'] = dataset_created_at_max

    # Previous datasets
    context['prev_datasets'] = json.loads(request.cookies.get('prev_datasets', '[]'))

    # Mode
    context['is_local_mode'] = _is_local_mode(request)
    # Whether a full extract
    context['full_dataset'] = full_dataset
    return context


def _search_to_search_context(search, dataset_params, tweet_limit=None, clear_cache=False):
    cache_context = redis.get(json.dumps(dataset_params))
    context = dict()
    if not cache_context or clear_cache:
        search_response = search.execute()
        context['total_tweets'] = search_response.hits.total.value if not tweet_limit else min(
            search_response.hits.total.value, tweet_limit)
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
        redis.set(json.dumps(dataset_params), json.dumps(context), ex=24 * 60 * 60)
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


def _dataset_path(dataset_id, full_dataset=False):
    if full_dataset:
        return os.path.join(app.config['FULL_DATASETS_PATH'], dataset_id)    
    return os.path.join(app.config['DATASETS_PATH'], dataset_id)


def _dataset_params_to_context(dataset_params):
    context = dict()
    for key, value in dataset_params.items():
        # Ignore email in dataset_params.json
        if key == 'requester_email':
            continue
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
    dataset_params['source_dataset'] = form['limit_source_datasets']
    if 'dataset_name' in form:
        dataset_params['dataset_name'] = form['dataset_name']
    if form.get('is_full_dataset'):
        dataset_params['is_full_dataset'] = True
    return dataset_params


def _tweet_count(clear_cache=False):
    tweet_count_str = redis.get('tweet_count')
    if not tweet_count_str or clear_cache:
        tweet_count = 0
        search = DatasetDocument.search()
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
        if status.lower() == 'red' or status.lower() == 'unknown':
            result = '<p class="text-danger">{}</p>'.format(status)
    if result:
        if eval_ctx.autoescape:
            return Markup(result)
        else:
            return result
    return status

def _create_extract(dataset_id):
    '''Given the unique identifier for a TweetSets dataset, submits a task to generate the extracts (ID\'s, JSON, CSV) for the whole dataset and saves to local storage.'''
    def get_file_limits():
        '''Returns the limits for each type of extract, as set in environment variables, or uses the default.'''
        return (app.config['MAX_PER_TXT_FILE'],
                app.config['MAX_PER_JSON_FILE'],
                app.config['MAX_PER_CSV_FILE'])
    
    def construct_path(dataset_id):
        '''Constructs the path to save the extract, given its unique ID, creating the directory if necessary.'''
        path = _dataset_path(dataset_id, full_dataset=True)
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def get_tweet_count(dataset_params):
        '''Retrieve total count of Tweets in dataset from ES index, using the dataset ID provided as a field in dataset_params.'''
        search = dataset_params_to_search(dataset_params, skip_aggs=True)
        search_response = search.execute()
        return search_response.hits.total.value
    # Dataset path
    dataset_path = construct_path(dataset_id)
    # Create the minimally necessary params for this extract
    dataset_params = {'tweet_type_original': 'true', 'tweet_type_quote': 'true', 'tweet_type_retweet': 'true', 'tweet_type_reply': 'true'}
    dataset_params.update({'source_dataset': dataset_id,
                            'dataset_name': 'full-extract-of-{}'.format(dataset_id)})
    # Save to disk 
    write_json(os.path.join(dataset_path, 'dataset_params.json'), dataset_params)
    # Create the task definitions
    keys = ['tweet_ids', 'tweet_json', 'tweet_csv']
    # File limits
    task_defs = {key: {'max_per_file': value} 
                    for key, value in zip(keys, get_file_limits())}
    # Email for notification
    task_defs['requester_email'] = app.config['ADMIN_EMAIL']
    # URL for extract
    task_defs['dataset_url'] = app.config['HOST']
    # Total tweets in index
    total_tweets = get_tweet_count(dataset_params)
    task = _generate_tasks.delay(task_defs, 
                                        dataset_params, 
                                        total_tweets, 
                                        dataset_path)
    generate_tasks_filepath = os.path.join(dataset_path, 'generate_tasks.json')
    write_json(generate_tasks_filepath, {'id': task.id})
    return 

# Command-line tool for creating full data extracts 
@app.cli.command('create-extract')
@click.argument('dataset_id')
def create_extract(dataset_id):
    _create_extract(dataset_id)

# Task
@celery.task(bind=True)
def _generate_tasks(self, task_defs, dataset_params, total_tweets, dataset_path, generate_update_increment=None):
    return tasks.generate_tasks(self, task_defs, dataset_params, total_tweets, dataset_path,
                                generate_update_increment=generate_update_increment)
