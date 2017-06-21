from elasticsearch_dsl.connections import connections
from elasticsearch import helpers
from elasticsearch_dsl import Search
from glob import glob
import gzip
import logging
import json
import argparse
from datetime import datetime
from time import sleep
import os

from models import TweetIndex, to_tweet, DatasetIndex, to_dataset, DatasetDocType
from utils import read_json, short_uid

log = logging.getLogger(__name__)

connections.create_connection(hosts=['elasticsearch'], timeout=20)


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser('tweetset_loader')
    parser.add_argument('--debug', action='store_true')

    # Subparsers
    subparsers = parser.add_subparsers(dest='command', help='command help')

    create_parser = subparsers.add_parser('create', help='create a dataset')
    create_parser.add_argument('--path', help='path of dataset', default='/dataset')
    create_parser.add_argument('--filename', help='filename of dataset file', default='dataset.json')

    update_parser = subparsers.add_parser('update', help='update dataset metadata')
    update_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    update_parser.add_argument('--path', help='path of dataset', default='/dataset')
    update_parser.add_argument('--filename', help='filename of dataset file', default='dataset.json')

    delete_parser = subparsers.add_parser('delete', help='delete dataset and tweets')
    delete_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')

    truncate_parser = subparsers.add_parser('truncate', help='delete tweets for a dataset')
    truncate_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')

    tweets_parser = subparsers.add_parser('tweets', help='add tweets to a dataset')
    tweets_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    tweets_parser.add_argument('--path', help='path of the directory containing the tweet files', default='/dataset')
    tweets_parser.add_argument('--limit', type=int, help='limit the number of tweets to load')
    tweets_parser.add_argument('--skip-count', action='store_true', help='skip count the tweets')
    tweets_parser.add_argument('--store-tweet', action='store_true', help='store the entire tweet')

    dataset_parser = subparsers.add_parser('dataset', help='create a dataset and add tweets')
    dataset_parser.add_argument('--path', help='path of dataset', default='/dataset')
    dataset_parser.add_argument('--filename', help='filename of dataset file', default='dataset.json')
    dataset_parser.add_argument('--limit', type=int, help='limit the number of tweets to load')
    dataset_parser.add_argument('--skip-count', action='store_true', help='skip count the tweets')
    dataset_parser.add_argument('--store-tweet', action='store_true', help='store the entire tweet')

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
    tweet_index = TweetIndex()
    tweet_index.create(ignore=400)

    dataset_id = None
    if args.command in ('create', 'dataset'):
        dataset = to_dataset(read_json(os.path.join(args.path, args.filename)),
                             dataset_id=short_uid(6,
                                                  exists_func=lambda uid: DatasetDocType.get(uid,
                                                                                             ignore=404) is not None))
        dataset.save()
        dataset_id = dataset.meta.id
        log.info('Created {}'.format(dataset.meta.id))
        print('Dataset id is {}'.format(dataset.meta.id))
    if args.command == 'update':
        dataset = DatasetDocType.get(args.dataset_identifier)
        if not dataset:
            raise Exception('{} not found'.format(args.dataset_identifier))
        updated_dataset = to_dataset(read_json(os.path.join(args.path, args.filename)), dataset)
        updated_dataset.save()
        log.info('Updated {}'.format(dataset.meta.id))
    if args.command == 'delete':
        dataset = DatasetDocType.get(args.dataset_identifier)
        if not dataset:
            raise Exception('{} not found'.format(args.dataset_identifier))
        dataset.delete()
        log.info('Deleted {}'.format(dataset.meta.id))
        search = Search(index='tweets')
        search = search.query('term', dataset_id=args.dataset_identifier)
        search.delete()
        log.info('Deleted tweets from {}'.format(dataset.meta.id))
    if args.command == 'truncate':
        search = Search(index='tweets')
        search = search.query('term', dataset_id=args.dataset_identifier)
        search.delete()
        log.info('Deleted tweets from {}'.format(args.dataset_identifier))
    if args.command in ('tweets', 'dataset'):
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
        tweet_count = None
        if not args.skip_count:
            log.info('Counting tweets in %s files.', file_count)
            tweet_count = count_lines(*filepaths)
            log.info('{:,} total tweets'.format(tweet_count))
        helpers.bulk(connections.get_connection(),
                     (to_tweet(tweet_json, dataset_id, store_tweet=store_tweet).to_dict(include_meta=True) for
                      tweet_json in
                      tweet_iter(*filepaths, limit=args.limit, total_tweets=tweet_count)))

        # Get number of tweets in dataset and update
        sleep(5)
        search = Search(index='tweets')
        search = search.query('term', dataset_id=dataset_id)[0:0]
        search.aggs.metric('created_at_min', 'min', field='created_at')
        search.aggs.metric('created_at_max', 'max', field='created_at')
        search_response = search.execute()
        dataset.first_tweet_created_at = datetime.utcfromtimestamp(
            search_response.aggregations.created_at_min.value / 1000.0)
        dataset.last_tweet_created_at = datetime.utcfromtimestamp(
            search_response.aggregations.created_at_max.value / 1000.0)
        dataset.tweet_count = search_response.hits.total
        dataset.save()
    if args.command == 'clear':
        dataset_index.delete()
        tweet_index.delete()
        log.info("Deleted indexes")
