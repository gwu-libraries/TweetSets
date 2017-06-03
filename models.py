from elasticsearch_dsl import DocType, Date, Boolean, \
    Keyword, Text, Index, Integer, MetaField
from dateutil.parser import parse as date_parse
from datetime import datetime
import uuid

class DatasetDocType(DocType):
    name = Text()
    description = Text()
    tags = Keyword()
    creators = Text()
    link = Keyword()
    created = Date()
    updated = Date()
    first_tweet_created_at = Date()
    last_tweet_created_at = Date()
    tweet_count = Integer()

    class Meta:
        index = 'datasets'

    def save(self, **kwargs):
        self.updated = datetime.now()
        return super().save(**kwargs)

def to_dataset(dataset_json, dataset=None):
    if not dataset:
        dataset = DatasetDocType()
        dataset.created = datetime.now()
        dataset.meta.id = uuid.uuid4().hex
    # This will throw a KeyError for missing, required fields
    dataset.name = dataset_json['name']
    dataset.description = dataset_json.get('description')
    dataset.tags = dataset_json.get('tags', [])
    dataset.creators = dataset_json.get('creators', [])
    dataset.link = dataset_json['link']
    return dataset

class TweetDocType(DocType):
    tweet_id = Keyword()
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

    class Meta:
        all = MetaField(enabled=False)
        index = 'tweets'
    # TODO: urls


def to_tweet(tweet_json, dataset_id):
    tweet = TweetDocType()
    tweet.meta.id = ':'.join([dataset_id, tweet_json['id_str']])
    tweet.tweet_id = tweet_json['id_str']
    tweet.dataset_id = dataset_id
    tweet.tweet_type = tweet_type(tweet_json)
    tweet.text = [tweet_text(tweet_json)]
    if tweet.tweet_type == 'quote':
        tweet.text.append(tweet_text(tweet_json['quoted_status']))
    tweet.created_at = date_parse(tweet_json['created_at'])
    tweet.user_id = tweet_json['user']['id_str']
    tweet.user_screen_name = tweet_json['user']['screen_name']
    tweet.user_follower_count = tweet_json['user']['followers_count']
    tweet.user_verified = tweet_json['user']['verified']
    tweet.mention_user_ids, tweet.mention_screen_names = mentions(tweet_json)
    tweet.hashtags = tweet_hashtags(tweet_json)
    tweet.favorite_count = tweet_json['favorite_count']
    tweet.retweet_count = tweet_json['retweet_count']
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


def tweet_hashtags(tweet_json):
    hashtags = []
    for hashtag in tweet_json['entities']['hashtags']:
        hashtags.append(hashtag['text'].lower())
    return hashtags


def mentions(tweet_json):
    mentions_user_ids = []
    mention_screen_names = []
    for mention in tweet_json['entities']['user_mentions']:
        mentions_user_ids.append(mention['id_str'])
        mention_screen_names.append(mention['screen_name'])
    return mentions_user_ids, mention_screen_names


class TweetIndex(Index):
    def __init__(self):
        Index.__init__(self, 'tweets')
        # register a doc_type with the index
        self.doc_type(TweetDocType)

class DatasetIndex(Index):
    def __init__(self):
        Index.__init__(self, 'datasets')
        # register a doc_type with the index
        self.doc_type(DatasetDocType)
