from elasticsearch_dsl import DocType, Date, Boolean, \
    Keyword, Text, Index, Integer, MetaField, Object
from dateutil.parser import parse as date_parse
from datetime import datetime
import uuid
import json


class DatasetDocType(DocType):
    name = Keyword()
    description = Text()
    tags = Keyword()
    creators = Text()
    link = Keyword()
    created = Date()
    updated = Date()
    first_tweet_created_at = Date()
    last_tweet_created_at = Date()
    tweet_count = Integer()
    local_only = Boolean()

    class Meta:
        index = 'datasets'

    def save(self, **kwargs):
        self.updated = datetime.now()
        return super().save(**kwargs)


def to_dataset(dataset_json, dataset=None, dataset_id=None):
    if not dataset:
        dataset = DatasetDocType()
        dataset.created = datetime.now()
        dataset.meta.id = dataset_id or uuid.uuid4().hex
    # This will throw a KeyError for missing, required fields
    dataset.name = dataset_json['name']
    dataset.description = dataset_json.get('description')
    dataset.tags = dataset_json.get('tags', [])
    dataset.creators = dataset_json.get('creators', [])
    dataset.link = dataset_json['link']
    dataset.local_only = dataset_json.get('local_only', False)
    return dataset


class TweetDocType(DocType):
    dataset_id = Keyword()
    text = Text()
    tweet_type = Keyword()
    created_at = Date()
    user_id = Keyword()
    user_screen_name = Keyword()
    user_follower_count = Integer()
    user_verified = Boolean()
    mention_user_ids = Keyword()
    mention_screen_names = Keyword()
    hashtags = Keyword()
    favorite_count = Integer()
    retweet_count = Integer()
    retweeted_quoted_user_id = Keyword()
    retweeted_quoted_screen_name = Keyword()
    in_reply_to_user_id = Keyword()
    in_reply_to_screen_name = Keyword()
    has_media = Boolean()
    urls = Keyword()
    has_geo = Boolean()
    tweet = Text(index=False)

    class Meta:
        all = MetaField(enabled=False)
        # Exclude storing the text field
        source = MetaField(excludes=['text'])


def to_tweet(tweet_json, dataset_id, index_name, store_tweet=False):
    entities = tweet_json.get('extended_tweet', {}).get('entities') or tweet_json['entities']

    tweet = TweetDocType()
    tweet.meta.id = tweet_json['id_str']
    tweet.meta.index = index_name
    tweet.dataset_id = dataset_id
    type = tweet_type(tweet_json)
    tweet.tweet_type = type
    text = [tweet_text(tweet_json)]
    if tweet.tweet_type == 'quote':
        text.append(tweet_text(tweet_json['quoted_status']))
        tweet.retweeted_quoted_user_id = tweet_json['quoted_status']['user']['id_str']
        tweet.retweeted_quoted_screen_name = tweet_json['quoted_status']['user']['screen_name']
    elif tweet.tweet_type == 'retweet':
        tweet.retweeted_quoted_user_id = tweet_json['retweeted_status']['user']['id_str']
        tweet.retweeted_quoted_screen_name = tweet_json['retweeted_status']['user']['screen_name']
    elif tweet.tweet_type == 'reply':
        tweet.in_reply_to_user_id = tweet_json.get('in_reply_to_user_id_str')
        tweet.in_reply_to_screen_name = tweet_json.get('in_reply_to_screen_name')
    tweet.text = tuple(text)
    tweet.created_at = date_parse(tweet_json['created_at'])
    tweet.user_id = tweet_json['user']['id_str']
    tweet.user_screen_name = tweet_json['user']['screen_name']
    tweet.user_follower_count = tweet_json['user']['followers_count']
    tweet.user_verified = tweet_json['user']['verified']
    tweet.mention_user_ids, tweet.mention_screen_names = mentions(entities)
    tweet.hashtags = tweet_hashtags(entities)
    tweet.favorite_count = tweet_json['favorite_count']
    tweet.retweet_count = tweet_json['retweet_count']
    tweet.has_media = 'media' in entities
    tweet.urls = urls(entities, type)
    tweet.has_geo = tweet.has_geo = True if tweet_json.get('geo') or tweet_json.get('place') or tweet_json.get(
        'coordinates') else False
    if store_tweet:
        tweet.tweet = json.dumps(tweet_json)
    return tweet


def tweet_type(tweet_json):
    # Determine the type of a tweet
    if tweet_json.get('in_reply_to_status_id'):
        return 'reply'
    if 'retweeted_status' in tweet_json:
        return 'retweet'
    if 'quoted_status' in tweet_json:
        return 'quote'
    return 'original'


def tweet_text(tweet_json):
    # This handles compat, extended, and extended streaming tweets.
    return tweet_json.get('full_text') \
           or tweet_json.get('extended_tweet', {}).get('full_text') \
           or tweet_json['text']


def tweet_hashtags(entities):
    hashtags = []
    for hashtag in entities['hashtags']:
        hashtags.append(hashtag['text'].lower())
    return tuple(hashtags)


def mentions(entities):
    mentions_user_ids = []
    mention_screen_names = []
    for mention in entities['user_mentions']:
        mentions_user_ids.append(mention['id_str'])
        mention_screen_names.append(mention['screen_name'])
    return tuple(mentions_user_ids), tuple(mention_screen_names)


def urls(entities, type):
    urls = []
    for url_obj in entities['urls']:
        url = url_obj.get('expanded_url') or url_obj['url']
        if url and (not type == 'quote' or not url.startswith('https://twitter.com/')):
            # Normalize to lower case and http
            urls.append(url.lower().replace('https://', 'http://'))
    return tuple(urls)


class TweetIndex(Index):
    def __init__(self, index_name, shards=1, replicas=1, refresh_interval=1):
        Index.__init__(self, index_name)
        self.settings(
            number_of_shards=shards,
            number_of_replicas=replicas,
            refresh_interval=refresh_interval
        )
        # register a doc_type with the index
        self.doc_type(TweetDocType)


def get_tweets_index_name(dataset_id):
    return 'tweets-{}'.format(dataset_id)


class DatasetIndex(Index):
    def __init__(self):
        Index.__init__(self, 'datasets')
        self.settings(
            number_of_shards=1
        )
        # register a doc_type with the index
        self.doc_type(DatasetDocType)
