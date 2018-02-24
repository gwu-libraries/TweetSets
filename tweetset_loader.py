from elasticsearch_dsl.connections import connections
from elasticsearch import helpers
from elasticsearch_dsl import Search, Index
from elasticsearch.exceptions import ConnectionError
from glob import glob
import gzip
import logging
import json
import argparse
from datetime import datetime
from time import sleep
import os
import itertools
import math
from collections import deque

from models import TweetIndex, to_tweet, DatasetIndex, to_dataset, DatasetDocType, get_tweets_index_name
from utils import read_json, short_uid

log = logging.getLogger(__name__)

connections.create_connection(hosts=['elasticsearch'], timeout=90, sniff_on_start=True, sniff_on_connection_fail=True,
                              retry_on_timeout=True)


def find_files(path):
    """
    Returns (.json files, .json.gz files, .txt files) found in path.
    """
    json_filepaths = glob('{}/*.json'.format(path))
    dataset_filepath = os.path.join(path, 'dataset.json')
    if dataset_filepath in json_filepaths:
        json_filepaths.remove(dataset_filepath)
    return (json_filepaths,
            glob('{}/*.json.gz'.format(path)),
            glob('{}/*.txt'.format(path)))


def count_lines(json_files, json_gz_files, txt_files):
    total_lines = 0
    for filepath in json_files:
        total_lines += sum(1 for _ in open(filepath))
    for filepath in json_gz_files:
        total_lines += sum(1 for _ in gzip.open(filepath))
    for filepath in txt_files:
        total_lines += sum(1 for _ in open(filepath))
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
        with gzip.open(filepath) as file:
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
    # If a exists then a is existing and b is new
    if Index(index_name_a).exists():
        return alias_name, index_name_b, index_name_a
    # Else if b exists then a is new and b is existing
    elif Index(index_name_b).exists():
        return alias_name, index_name_b, index_name_a
    # Else neither exists and a is new
    return alias_name, index_name_a, None


def update_dataset_stats(dataset):
    search = Search(index=get_tweets_index_name(dataset.meta.id))
    search = search.query('term', dataset_id=dataset.meta.id)[0:0]
    search.aggs.metric('created_at_min', 'min', field='created_at')
    search.aggs.metric('created_at_max', 'max', field='created_at')
    search_response = search.execute()
    dataset.first_tweet_created_at = datetime.utcfromtimestamp(
        search_response.aggregations.created_at_min.value / 1000.0)
    dataset.last_tweet_created_at = datetime.utcfromtimestamp(
        search_response.aggregations.created_at_max.value / 1000.0)
    dataset.tweet_count = search_response.hits.total
    dataset.save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('tweetset_loader')
    parser.add_argument('--debug', action='store_true')

    # Subparsers
    subparsers = parser.add_subparsers(dest='command', help='command help')

    update_parser = subparsers.add_parser('update', help='update dataset metadata')
    update_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    update_parser.add_argument('--path', help='path of dataset', default='/dataset')
    update_parser.add_argument('--filename', help='filename of dataset file', default='dataset.json')
    update_parser.add_argument('--stats', action='store_true', help='Also update dataset statistics')
    update_parser.add_argument('--create', action='store_true', help='Create if does not exist.')

    delete_parser = subparsers.add_parser('delete', help='delete dataset and tweets')
    delete_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')

    reload_parser = subparsers.add_parser('reload', help='reload the tweets in a dataset')
    reload_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    reload_parser.add_argument('--path', help='path of the directory containing the tweet files', default='/dataset')
    reload_parser.add_argument('--limit', type=int, help='limit the number of tweets to load')
    reload_parser.add_argument('--skip-count', action='store_true', help='skip count the tweets')
    reload_parser.add_argument('--store-tweet', action='store_true', help='store the entire tweet')
    reload_parser.add_argument('--replicas', type=int, default='1', help='number of replicas to make of this dataset')

    dataset_parser = subparsers.add_parser('create', help='create a dataset and add tweets')
    dataset_parser.add_argument('--path', help='path of dataset', default='/dataset')
    dataset_parser.add_argument('--filename', help='filename of dataset file', default='dataset.json')
    dataset_parser.add_argument('--limit', type=int, help='limit the number of tweets to load')
    dataset_parser.add_argument('--skip-count', action='store_true', help='skip count the tweets')
    dataset_parser.add_argument('--store-tweet', action='store_true', help='store the entire tweet')
    dataset_parser.add_argument('--shards', type=int, help='number of shards for this dataset')
    dataset_parser.add_argument('--replicas', type=int, default='1', help='number of replicas to make of this dataset')

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
    dataset_index.create(ignore=400)

    dataset_id = None
    if args.command == 'delete':
        dataset = DatasetDocType.get(args.dataset_identifier)
        if not dataset:
            raise Exception('{} not found'.format(args.dataset_identifier))
        dataset.delete()
        log.info('Deleted {}'.format(dataset.meta.id))
        delete_tweet_index(args.dataset_identifier)
    if args.command == 'create':
        dataset = to_dataset(read_json(os.path.join(args.path, args.filename)),
                             dataset_id=short_uid(6,
                                                  exists_func=lambda uid: DatasetDocType.get(uid,
                                                                                             ignore=404) is not None))
        dataset.save()
        dataset_id = dataset.meta.id
        log.info('Created {}'.format(dataset_id))
        print('Dataset id is {}'.format(dataset_id))

    if args.command in ('create', 'reload'):
        if dataset_id is None:
            dataset_id = args.dataset_identifier
        store_tweet = os.environ.get('STORE_TWEET', 'false').lower() == 'true' or args.store_tweet
        if store_tweet:
            log.info('Storing tweet')
        dataset = DatasetDocType.get(dataset_id)
        if not dataset:
            raise Exception('{} not found'.format(dataset_id))

        filepaths = find_files(args.path)
        file_count = count_files(*filepaths)
        tweet_count = 0
        if not args.skip_count:
            log.info('Counting tweets in %s files.', file_count)
            tweet_count = count_lines(*filepaths)
            log.info('{:,} total tweets'.format(tweet_count))

        # Create the index
        # In testing, 500K tweets (storing tweet) = 615MB
        # Thus, 32.5 million tweets per shard to have a max shard size of 40GB
        # In testing, 500k tweets (not storing tweet) = 145MB
        # Thus, 138 million tweets per shard to have a max shard size of 40GB
        tweets_per_shard = 32500000 if store_tweet else 138000000
        shards = (args.shards if hasattr(args, 'shards') else None) or math.ceil(
            float(tweet_count) / tweets_per_shard) or 1
        log.info('Using %s shards and %s replicas for index.', shards, args.replicas)

        alias_name, new_index_name, existing_index_name = get_tweet_index_state(dataset_id)

        log.debug('Index name is %s', new_index_name)
        tweet_index = TweetIndex(new_index_name, shards=shards, replicas=0, refresh_interval=-1)
        tweet_index.create()

        # create_tweet_index(dataset_id, shards, replicas=0, )
        connection = connections.get_connection()

        deque(helpers.parallel_bulk(connection,
                     [to_tweet(tweet_json, dataset_id, new_index_name, store_tweet=store_tweet).to_dict(
                         include_meta=True) for
                      tweet_json in
                      tweet_iter(*filepaths, limit=args.limit, total_tweets=tweet_count)]), maxlen=0)

        log.debug('Setting replicas and refresh interval')
        tweet_index.put_settings(body={
            'number_of_replicas': args.replicas, 'refresh_interval': '1s'})

        # Add aliases
        log.debug('Adding alias %s to %s', alias_name, new_index_name)
        tweet_index.put_alias(name=alias_name)

        # Delete existing index
        if existing_index_name:
            log.debug('Deleting existing index %s', existing_index_name)
            TweetIndex(existing_index_name).delete()

        # Get number of tweets in dataset and update
        sleep(5)
        update_dataset_stats(dataset)

    if args.command == 'clear':
        search = DatasetDocType.search()
        for dataset in search.execute():
            delete_tweet_index(dataset.meta.id)
        dataset_index.delete()
        log.info("Deleted indexes")
