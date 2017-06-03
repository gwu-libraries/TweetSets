from elasticsearch_dsl.connections import connections
from elasticsearch import helpers
from elasticsearch_dsl import Search
from glob import glob
import os
import gzip
import logging
import json
import argparse
from datetime import datetime

from models import TweetIndex, to_tweet, DatasetIndex, to_dataset, DatasetDocType
from tweetset_server import _read_json

log = logging.getLogger(__name__)

connections.create_connection(hosts=['localhost'], timeout=20)

def find_files(path):
    """
    Returns (.json files, .json.gz files, .txt files) found in path.
    """
    # json_filepaths = []
    # json_gz_filepaths = []
    # txt_filepaths = []
    # for filename in os.listdir(path):
    #     filepath = os.path.join(path, filename)
    #     if filename.lower().endswith('.json'):
    #         json_filepaths.append(filepath)
    #     elif filename.lower().endswith('.json.gz'):
    #         json_gz_filepaths.append(filepath)
    #     elif filepath.lower().endswith('.txt'):
    #         txt_filepaths.append(filepath)
    # return json_filepaths, json_gz_filepaths, txt_filepaths
    return (glob('{}/*.json'.format(path)),
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


def tweet_iter(json_files, json_gz_files, txt_files, limit=None):
    counter = 0
    for filepath in json_files:
        with open(filepath) as file:
            for line in file:
                if counter % 10000 == 0:
                    log.info('%s tweets', counter)
                if counter == limit:
                    break
                counter += 1
                yield json.loads(line)
    for filepath in json_gz_files:
        with gzip.open(filepath) as file:
            for line in file:
                if counter % 10000 == 0:
                    log.info('%s tweets', counter)
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
    create_parser.add_argument('dataset_filepath', help='filepath of dataset file')

    update_parser = subparsers.add_parser('update', help='update dataset metadata')
    update_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    update_parser.add_argument('dataset_filepath', help='filepath of dataset file')

    delete_parser = subparsers.add_parser('delete', help='delete dataset and tweets')
    delete_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')

    truncate_parser = subparsers.add_parser('truncate', help='delete tweets for a dataset')
    truncate_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')

    tweets_parser = subparsers.add_parser('tweets', help='add tweets to a dataset')
    tweets_parser.add_argument('dataset_identifier', help='identifier (a UUID) for the dataset')
    tweets_parser.add_argument('tweet_files_path', help='path of the directory containing the tweet files')
    tweets_parser.add_argument('--limit', type=int, help='limit the number of tweets to load')

    args = parser.parse_args()

    # Logging
    logging.getLogger("urllib3").setLevel(logging.WARNING)
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

    if args.command == 'create':
        dataset = to_dataset(_read_json(args.dataset_filepath))
        dataset.save()
        log.info('Created {}'.format(dataset.meta.id))
        print('Dataset id is {}'.format(dataset.meta.id))
    elif args.command == 'update':
        dataset = DatasetDocType.get(args.dataset_identifier)
        if not dataset:
            raise Exception('{} not found'.format(args.dataset_identifier))
        updated_dataset = to_dataset(_read_json(args.dataset_filepath), dataset)
        updated_dataset.save()
        log.info('Updated {}'.format(dataset.meta.id))
    elif args.command == 'delete':
        dataset = DatasetDocType.get(args.dataset_identifier)
        if not dataset:
            raise Exception('{} not found'.format(args.dataset_identifier))
        dataset.delete()
        log.info('Deleted {}'.format(dataset.meta.id))
        search = Search(index='tweets')
        search = search.query('term', dataset_id=args.dataset_identifier)
        search.delete()
        log.info('Deleted tweets from {}'.format(dataset.meta.id))
    elif args.command == 'truncate':
        pass
    elif args.command == 'tweets':
        dataset = DatasetDocType.get(args.dataset_identifier)
        if not dataset:
            raise Exception('{} not found'.format(args.dataset_identifier))
        filepaths = find_files(args.tweet_files_path)
        file_count = count_files(*filepaths)
        log.info('Counting tweets in %s files.', file_count)
        tweet_count = count_lines(*filepaths)
        log.info("%s total tweets", tweet_count)
        helpers.bulk(connections.get_connection(), (to_tweet(tweet_json, args.dataset_identifier).to_dict(include_meta=True) for tweet_json in tweet_iter(*filepaths, limit=args.limit)))

        # Get number of tweets in dataset and update
        search = Search(index='tweets')
        search = search.query('term', dataset_id=args.dataset_identifier)[0:0]
        search.aggs.metric('created_at_min', 'min', field='created_at')
        search.aggs.metric('created_at_max', 'max', field='created_at')
        search_response = search.execute()
        dataset.first_created_at = datetime.utcfromtimestamp(search_response.aggregations.created_at_min.value / 1000.0)
        dataset.last_created_at = datetime.utcfromtimestamp(search_response.aggregations.created_at_max.value / 1000.0)
        dataset.tweet_count = search_response.hits.total
        dataset.save()

    # for counter, tweet_json in enumerate(tweet_iter(*filepaths, limit=5)):
    # #     print(counter)
    #     tweet = from_tweet_json(tweet_json)
    #     log.info(tweet.to_dict(include_meta=True))
    # #     tweet.save()
    # # find_files('test_data')
    # helpers.bulk(connections.get_connection(), (to_tweet(tweet_json).to_dict(include_meta=True) for tweet_json in tweet_iter(*filepaths, limit=50000)))

    # TODO: Figure out updates
    # TODO: Don't save source
    # TODO: Log tweet count as loading