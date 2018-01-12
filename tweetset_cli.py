from elasticsearch_dsl.connections import connections
from utils import dataset_params_to_search
import json
import argparse
import sys

connections.create_connection(hosts=['localhost'], timeout=90)


def fetch_by_screen_name(screen_name, source_datasets):
    search = dataset_params_to_search({
        'source_datasets': source_datasets,
        'tweet_type_original': 'true',
        'tweet_type_reply': 'true',
        'tweet_type_retweet': 'true',
        'tweet_type_quote': 'true',
        'poster_any': screen_name.lstrip('@')
    }, skip_aggs=True)
    search.source(['tweet'])

    for hit in search.scan():
        yield hit.tweet.to_dict()


def fetch_by_source_screen_name(screen_name, source_datasets):
    search = dataset_params_to_search({
        'source_datasets': source_datasets,
        'tweet_type_retweet': 'true',
        'tweet_type_quote': 'true',
        'source_poster_any': screen_name.lstrip('@')
    }, skip_aggs=True)
    search.source(['tweet'])

    for hit in search.scan():
        tweet = hit.tweet.to_dict()
        if 'retweeted_status' in tweet:
            yield tweet['retweeted_status']
        elif 'quoted_status' in tweet:
            yield tweet['quoted_status']


def fetch_by_mention_screen_name(mention_screen_name, source_datasets):
    search = dataset_params_to_search({
        'source_datasets': source_datasets,
        'tweet_type_original': 'true',
        'tweet_type_reply': 'true',
        'tweet_type_retweet': 'true',
        'tweet_type_quote': 'true',
        'mention_any': mention_screen_name.lstrip('@')
    }, skip_aggs=True)
    search.source(['tweet'])

    for hit in search.scan():
        yield hit.tweet.to_dict()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('datasets', help='comma separated list of dataset ids')
    # parser.add_argument('--output-dir', help='default is {}'.format(os.getcwd()), default=os.getcwd())
    subparsers = parser.add_subparsers(dest='command', help='command help')

    by_screen_name_parser = subparsers.add_parser('by_screen_name',
                                                  help='Fetch tweets posted by user with provided screen name.')
    by_screen_name_parser.add_argument('screen_name')

    by_source_screen_name_parser = subparsers.add_parser('by_source_screen_name',
                                                         help='Fetch tweets that are posted by user with provided '
                                                              'screen name by looking in retweets or quotes of those '
                                                              'tweets. The source tweet is extracted from the retweet '
                                                              'or quote.')
    by_source_screen_name_parser.add_argument('screen_name')

    by_mention_screen_name_parser = subparsers.add_parser('by_mention_screen_name',
                                                          help='Fetch tweets in which user with provided screen name '
                                                               'is mentioned.')
    by_mention_screen_name_parser.add_argument('screen_name')

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)
    elif args.command == 'by_screen_name':
        tweets = fetch_by_screen_name(args.screen_name, args.datasets.split(','))
    elif args.command == 'by_source_screen_name':
        tweets = fetch_by_source_screen_name(args.screen_name, args.datasets.split(','))
    elif args.command == 'by_mention_screen_name':
        tweets = fetch_by_mention_screen_name(args.screen_name, args.datasets.split(','))

    for tweet in tweets:
        print(json.dumps(tweet))
