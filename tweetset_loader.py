from elasticsearch_dsl.connections import connections
from elasticsearch import helpers
from elasticsearch_dsl import Search, Index
from glob import glob
import gzip
import logging
import json
import argparse
from datetime import datetime
from time import sleep
import os
import math
import multiprocessing
import os.path
from pyspark.sql import SparkSession
from models import TweetIndex, to_tweet, DatasetIndex, to_dataset, DatasetDocument, TweetDocument, get_tweets_index_name
from utils import read_json, short_uid
from spark_utils import *
from shutil import copy

log = logging.getLogger(__name__)

connections.create_connection(hosts=[os.environ.get('ES_HOST', 'elasticsearch')], timeout=90, sniff_on_start=True,
                              sniff_on_connection_fail=True,
                              retry_on_timeout=True, maxsize=25)


def find_files(path):
    """
    Returns (.json files, .json.gz files, .txt files) found in path.
    """
    json_filepaths = glob('{}/*.json'.format(path))
    json_filepaths.extend(glob('{}/*.jsonl'.format(path)))
    dataset_filepath = os.path.join(path, 'dataset.json')
    if dataset_filepath in json_filepaths:
        json_filepaths.remove(dataset_filepath)
    gz_filepaths = glob('{}/*.json.gz'.format(path))
    gz_filepaths.extend(glob('{}/*.jsonl.gz'.format(path)))
    return (json_filepaths,
            gz_filepaths,
            glob('{}/*.txt'.format(path)))

def copy_json(json_files, dataset_id):
    '''
    Copies JSON (zipped and unzipped) files to the path for full TS extracts.
    :param files: list of files to be moved
    :param dataset_id: 6-character unique ID for this dataset
    '''
    full_dataset_path = os.environ.get('PATH_TO_EXTRACTS')
    if not full_dataset_path:
        log.error('ENV missing: PATH_TO_EXTRACTS. JSON extracts not copied.')
        return
    json_extract_dir = os.path.join(full_dataset_path, dataset_id, 'tweet-json')
    if not os.path.isdir(json_extract_dir):
        os.mkdirs(json_extract_dir)
    log.info(f'Copying {len(json_files)} JSON files to {json_extract_dir}.')
    for file in json_files:
        copy(file, json_extract_dir)
    return
    
def count_normal_lines(filepath):
    return sum(1 for _ in open(filepath))


def count_gz_lines(filepath):
    return sum(1 for _ in gzip.open(filepath))


def count_lines(json_files, json_gz_files, txt_files, threads=6):
    total_lines = 0
    pool = multiprocessing.Pool(threads)
    total_lines += sum(pool.imap_unordered(count_normal_lines, json_files))
    total_lines += sum(pool.imap_unordered(count_gz_lines, json_gz_files))
    total_lines += sum(pool.imap_unordered(count_normal_lines, txt_files))
    return total_lines


def count_files(json_files, json_gz_files, txt_files):
    return len(json_files) + len(json_gz_files) + len(txt_files)


def tweet_iter(json_files, json_gz_files, txt_files, limit=None, total_tweets=None):
    counter = 0
    for filepath in json_files:
        with open(filepath) as file:
            for line in file:
                if counter % 10000 == 0:
                    log.info('{:,} of {:,} tweets'.format(counter, limit or total_tweets or 0))
                if counter == limit:
                    break
                counter += 1
                yield json.loads(line)
    for filepath in json_gz_files:
        with gzip.open(filepath, 'rt') as file:
            for line in file:
                if counter % 10000 == 0:
                    log.info('{:,} of {:,} tweets'.format(counter, limit or total_tweets or 0))
                if counter == limit:
                    break
                counter += 1
                yield json.loads(line)

                # TODO: Handle hydration


def delete_tweet_index(dataset_identifier):
    _, __, existing_index_name = get_tweet_index_state(dataset_identifier)
    if existing_index_name:
        TweetIndex(existing_index_name).delete(ignore=404)
        log.info('Deleted tweets from {}'.format(dataset_identifier))


def get_tweet_index_state(dataset_identifier):
    """
    Returns alias name, new index name, existing index name or none
    """
    alias_name = get_tweets_index_name(dataset_identifier)
    index_name_a = '{}-a'.format(alias_name)
    index_name_b = '{}-b'.format(alias_name)
    existing_index = None
    new_index = index_name_a
    # If a exists then a is existing and b is new
    if Index(index_name_a).exists():
        existing_index = index_name_a
        new_index = index_name_b
    # Else if b exists then a is new and b is existing
    elif Index(index_name_b).exists():
        existing_index = index_name_b
    # Handle legacy cases where no existing alias
    elif Index(alias_name).exists():
        existing_index = alias_name
    # Else neither exists and a is new
    return alias_name, new_index, existing_index


def update_dataset_stats(dataset):
    search = Search(index=get_tweets_index_name(dataset.meta.id)).extra(track_total_hits=True)
    search = search.query('term', dataset_id=dataset.meta.id)[0:0]
    search.aggs.metric('created_at_min', 'min', field='created_at')
    search.aggs.metric('created_at_max', 'max', field='created_at')
    search_response = search.execute()
    dataset.first_tweet_created_at = datetime.utcfromtimestamp(
        search_response.aggregations.created_at_min.value / 1000.0)
    dataset.last_tweet_created_at = datetime.utcfromtimestamp(
        search_response.aggregations.created_at_max.value / 1000.0)
    dataset.tweet_count = search_response.hits.total.value
    dataset.save()


def clean_tweet_dict(tweet_dict):
    new_tweet_dict = tweet_dict['_source']
    new_tweet_dict['created_at'] = tweet_dict['_source']['created_at'].isoformat()
    new_tweet_dict['tweet_id'] = tweet_dict['_id']
    return new_tweet_dict


def shard_count(tweet_count, store_tweet=True):
    # In testing, 500K tweets (storing tweet) = 615MB
    # Thus, 32.5 million tweets per shard to have a max shard size of 40GB
    # In testing, 500k tweets (not storing tweet) = 145MB
    # Thus, 138 million tweets per shard to have a max shard size of 40GB
    tweets_per_shard = 32500000 if store_tweet else 138000000
    return math.ceil(float(tweet_count) / tweets_per_shard) or 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser('tweetset_loader')
    parser.add_argument('--debug', action='store_true')

    # Subparsers
    subparsers = parser.add_subparsers(dest='command', help='command help')

    update_parser = subparsers.add_parser('update', help='update dataset metadata (including statistics)')
    update_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    update_parser.add_argument('path', help='path of dataset')
    update_parser.add_argument('--filename', help='filename of dataset file', default='dataset.json')

    delete_parser = subparsers.add_parser('delete', help='delete dataset and tweets')
    delete_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')

    reload_parser = subparsers.add_parser('reload', help='reload the tweets in a dataset')
    reload_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    reload_parser.add_argument('path', help='path of the directory containing the tweet files')
    reload_parser.add_argument('--limit', type=int, help='limit the number of tweets to load')
    reload_parser.add_argument('--skip-count', action='store_true', help='skip count the tweets')
    reload_parser.add_argument('--store-tweet', action='store_true', help='store the entire tweet')
    reload_parser.add_argument('--replicas', type=int, default='1', help='number of replicas to make of this dataset')
    reload_parser.add_argument('--threads', type=int, default='2', help='number of loading threads')
    reload_parser.add_argument('--chunk-size', type=int, default='500', help='size of indexing chunk')

    spark_reload_parser = subparsers.add_parser('spark-reload', help='reload the tweets in a dataset using spark')
    spark_reload_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    spark_reload_parser.add_argument('path', help='path of the directory containing the tweet files')
    spark_reload_parser.add_argument('--skip-count', action='store_true', help='skip count the tweets')
    spark_reload_parser.add_argument('--store-tweet', action='store_true', help='store the entire tweet')
    spark_reload_parser.add_argument('--replicas', type=int, default='1',
                                     help='number of replicas to make of this dataset')

    dataset_parser = subparsers.add_parser('create', help='create a dataset and add tweets')
    dataset_parser.add_argument('path', help='path of dataset')
    dataset_parser.add_argument('--filename', help='filename of dataset file', default='dataset.json')
    dataset_parser.add_argument('--limit', type=int, help='limit the number of tweets to load')
    dataset_parser.add_argument('--skip-count', action='store_true', help='skip count the tweets')
    dataset_parser.add_argument('--store-tweet', action='store_true', help='store the entire tweet')
    dataset_parser.add_argument('--shards', type=int, help='number of shards for this dataset')
    dataset_parser.add_argument('--replicas', type=int, default='1', help='number of replicas to make of this dataset')
    dataset_parser.add_argument('--threads', type=int, default='2', help='number of loading threads')
    dataset_parser.add_argument('--chunk-size', type=int, default='500', help='size of indexing chunk')
    dataset_parser.add_argument('--dataset-identifier', help='identifier (a UUID) for the dataset')

    spark_create_parser = subparsers.add_parser('spark-create', help='create a dataset and add tweets using spark')
    spark_create_parser.add_argument('path', help='path of dataset')
    spark_create_parser.add_argument('--filename', help='filename of dataset file', default='dataset.json')
    spark_create_parser.add_argument('--skip-count', action='store_true', help='skip count the tweets')
    spark_create_parser.add_argument('--store-tweet', action='store_true', help='store the entire tweet')
    spark_create_parser.add_argument('--shards', type=int, help='number of shards for this dataset')
    spark_create_parser.add_argument('--replicas', type=int, default='1',
                                     help='number of replicas to make of this dataset')
    spark_create_parser.add_argument('--dataset-identifier', help='identifier (a UUID) for the dataset')

    subparsers.add_parser('clear', help='delete all indexes')

    args = parser.parse_args()

    # Logging
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO
    )

    if not args.command:
        parser.print_help()
        quit()

    # Create indexs if they doesn't exist
    dataset_index = DatasetIndex()
    dataset_index.document(DatasetDocument)
    dataset_index.create(ignore=400)

    dataset_id = args.dataset_identifier if hasattr(args, 'dataset_identifier') else None
    tweet_index = None
    dataset = None
    if args.command == 'delete':
        dataset = DatasetDocument.get(dataset_id)
        if not dataset:
            raise Exception('{} not found'.format(args.dataset_id))
        dataset.delete()
        log.info('Deleted {}'.format(dataset.meta.id))
        delete_tweet_index(dataset_id)
    if args.command == 'update':
        dataset = to_dataset(read_json(os.path.join(args.path, args.filename)),
                             dataset_id=dataset_id)
        dataset.save()
        update_dataset_stats(dataset)
        log.info('Updated dataset {}'.format(dataset_id))
        print('Updated dataset {}'.format(dataset_id))

    if args.command in ('create', 'spark-create'):
        if dataset_id is None:
            dataset_id = short_uid(6, exists_func=lambda uid: DatasetDocument.get(uid, ignore=404) is not None)
        dataset = to_dataset(read_json(os.path.join(args.path, args.filename)),
                             dataset_id=dataset_id)
        dataset.save()
        log.info('Created {}'.format(dataset_id))
        print('Dataset id is {}'.format(dataset_id))

    if args.command in ('create', 'reload'):
        store_tweet = os.environ.get('STORE_TWEET', 'false').lower() == 'true' or args.store_tweet
        if store_tweet:
            log.info('Storing tweet')
        dataset = DatasetDocument.get(dataset_id)
        if not dataset:
            raise Exception('{} not found'.format(dataset_id))

        filepaths = find_files(args.path)
        file_count = count_files(*filepaths)
        tweet_count = 0
        if not args.skip_count:
            log.info('Counting tweets in %s files.', file_count)
            tweet_count = count_lines(*filepaths)
            log.info('{:,} total tweets'.format(tweet_count))

        shards = (args.shards if hasattr(args, 'shards') else None) or shard_count(tweet_count, store_tweet)
        log.info('Using %s shards and %s replicas for index.', shards, args.replicas)

        alias_name, new_index_name, existing_index_name = get_tweet_index_state(dataset_id)

        log.debug('Index name is %s', new_index_name)
        tweet_index = TweetIndex(new_index_name, shards=shards, replicas=0, refresh_interval=-1)
        tweet_index.document(TweetDocument)
        tweet_index.create()

        log.debug('Indexing using %s threads', args.threads)
        for success, info in helpers.parallel_bulk(connections.get_connection(),
                                                   (to_tweet(tweet_json, dataset_id, new_index_name,
                                                             store_tweet=store_tweet).to_dict(
                                                       include_meta=True) for
                                                           tweet_json in
                                                           tweet_iter(*filepaths, limit=args.limit,
                                                                      total_tweets=tweet_count)),
                                                   thread_count=args.threads, chunk_size=args.chunk_size):
            log.debug('Success: %s. %s', success, info)

    if args.command in ('spark-create', 'spark-reload'):
        store_tweet = os.environ.get('STORE_TWEET', 'false').lower() == 'true' or args.store_tweet
        if store_tweet:
            log.info('Storing tweet')
        dataset = DatasetDocument.get(dataset_id)
        if not dataset:
            raise Exception('{} not found'.format(dataset_id))

        filepaths = find_files(args.path)
        file_count = count_files(*filepaths)
        tweet_count = 0
        if not args.skip_count:
            log.info('Counting tweets in %s files.', file_count)
            tweet_count = count_lines(*filepaths)
            log.info('{:,} total tweets'.format(tweet_count))

        tweets_per_shard = 32500000 if store_tweet else 138000000
        shards = (args.shards if hasattr(args, 'shards') else None) or max(shard_count(tweet_count, store_tweet), 4)
        log.info('Using %s shards and %s replicas for index.', shards, args.replicas)

        alias_name, new_index_name, existing_index_name = get_tweet_index_state(dataset_id)

        log.debug('Index name is %s', new_index_name)
        tweet_index = TweetIndex(new_index_name, shards=shards, replicas=0, refresh_interval=-1)
        tweet_index.document(TweetDocument)
        tweet_index.create()
        spark = SparkSession.builder.appName('TweetSets').getOrCreate()
        # Make Spark v3 use the v2 time parser
        # TO DO --> update Spark SQL code to use the new time parser
        spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
        # Set UTC as the time zone
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        # List of JSON files to load (for Spark, loads only *.json and *.gz)
        filepath_list = []
        filepath_list.extend(filepaths[0])
        filepath_list.extend(filepaths[1])
        # path at which to store full extracts
        full_dataset_path = os.environ.get('PATH_TO_EXTRACTS')
        full_dataset_path = os.path.join(full_dataset_path, dataset_id)
        if not os.path.isdir(full_dataset_path):
            os.mkdir(full_dataset_path)
        try:
            es_conf = {"es.nodes": os.environ.get('ES_HOST', 'elasticsearch'),
                       "es.port": "9200",
                       "es.index.auto.create": "false",
                       "es.mapping.id": "tweet_id",
                       "es.resource": "{}/_doc".format(new_index_name)}

            def to_tweet_dict(tweet_str):
                return clean_tweet_dict(
                    to_tweet(json.loads(tweet_str), dataset_id, '', store_tweet=True).to_dict(include_meta=True))
            tweets_str_rdd = spark.sparkContext.textFile(','.join(filepath_list))
            tweets_rdd = tweets_str_rdd.map(to_tweet_dict).map(lambda row: (row['tweet_id'], row))
            log.info('Saving tweets to Elasticsearch.')
            tweets_rdd.saveAsNewAPIHadoopFile(
                path='-',
                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                keyClass="org.apache.hadoop.io.NullWritable",
                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                conf=es_conf)
            # Load Spark schema and SQL for preparing TweetSets docs for Elasticsearch
            ts_schema = load_schema('./tweetsets_schema.json')
            ts_sql = load_sql('./tweetsets_sql_exp.sql')
            df = make_spark_df(spark, 
                                schema=ts_schema, 
                                sql=ts_sql, 
                                path_to_dataset=filepath_list,
                                dataset_id=dataset_id)
            # Create and save tweet ID's
            tweet_ids_path = os.path.join(full_dataset_path, 'tweet-ids')
            log.info(f'Saving tweet IDs to {tweet_ids_path}.')
            extract_tweet_ids(df, tweet_ids_path)
            # Create and save tweet CSV
            tweet_csv_path = os.path.join(full_dataset_path, 'tweet-csv')
            log.info(f'Saving CSV extracts to {tweet_csv_path}.')
            # Setting the escape character to the double quote. Otherwise, it causes problems for applications reading the CSV.
            extract_csv(df).write.option("header", "true").csv(tweet_csv_path, compression='gzip', escape='"')
            # Create and save mentions
            mentions_path = os.path.join(full_dataset_path, 'tweet-mentions')
            log.info(f'Saving tweet mentions to {mentions_path}.')
            mention_nodes, mention_edges = extract_mentions(df, spark)
            mention_nodes.write.option("header", "true").csv(os.path.join(mentions_path, 'nodes'), compression='gzip')
            mention_edges.write.option("header", "true").csv(os.path.join(mentions_path, 'edges'), compression='gzip')
            agg_mentions(df, spark).write.option("header", "true").csv(os.path.join(mentions_path, 'agg-mentions'), compression='gzip')
            # Create and save user counts
            users_path = os.path.join(full_dataset_path, 'tweet-users')
            agg_users(df, spark).write.option('header', 'true').csv(users_path, compression='gzip', escape='"')
        finally:
            spark.stop()
        # Copy full JSON tweet files to extracts directory
        copy_json(filepath_list, dataset_id)

    if args.command in ('create', 'reload', 'spark-create', 'spark-reload'):

        log.debug('Setting replicas and refresh interval')
        tweet_index.put_settings(body={
            'number_of_replicas': args.replicas, 'refresh_interval': '1s'})

        # Delete existing index
        if existing_index_name:
            log.debug('Deleting existing index %s', existing_index_name)
            TweetIndex(existing_index_name).delete()

        # Add aliases
        log.debug('Adding alias %s to %s', alias_name, new_index_name)
        tweet_index.put_alias(name=alias_name)

        # Get number of tweets in dataset and update
        sleep(5)
        update_dataset_stats(dataset)

    if args.command == 'clear':
        search = DatasetDocument.search()
        for dataset in search.execute():
            delete_tweet_index(dataset.meta.id)
        dataset_index.delete()
        log.info("Deleted indexes")
