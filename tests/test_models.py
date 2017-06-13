from unittest import TestCase
from datetime import datetime
from pytz import utc
from models import to_tweet
from .tweets import original_compat_tweet, original_extended_tweet, original_tweet_with_hashtag, \
    original_tweet_with_media, original_tweet_with_mention, original_tweet_with_url, reply_tweet, retweet, \
    quote_tweet, streaming_extended_tweet, geotagged_tweet


class TestTweet(TestCase):
    def test_original_extended_tweet(self):
        tweet = to_tweet(original_extended_tweet, 'test')
        self.assertEqual('test:577866396094242816', tweet.meta.id)
        self.assertEqual('original', tweet.tweet_type)
        self.assertEqual(['Test tweet 1'], tweet.text)
        self.assertEqual(datetime(2015, 3, 17, 16, 17, 39, tzinfo=utc), tweet.created_at)
        self.assertEqual('2875189485', tweet.user_id)
        self.assertEqual('jlittman_dev', tweet.user_screen_name)
        self.assertEqual(5, tweet.user_follower_count)
        self.assertFalse(tweet.user_verified)
        self.assertEqual(6, tweet.retweet_count)
        self.assertEqual(3, tweet.favorite_count)
        self.assertFalse(tweet.has_geo)

    def test_original_compat_tweet(self):
        tweet = to_tweet(original_compat_tweet, 'test')
        self.assertEqual(['Test tweet 1'], tweet.text)

    def test_tweet_with_hashtag(self):
        tweet = to_tweet(original_tweet_with_hashtag, 'test')
        self.assertEqual(['hashtag'], tweet.hashtags)

    def test_tweet_with_media(self):
        tweet = to_tweet(original_tweet_with_media, 'test')
        self.assertTrue(tweet.has_media)

    def test_tweet_with_mention(self):
        tweet = to_tweet(original_tweet_with_mention, 'test')
        self.assertEqual(['481186914'], tweet.mention_user_ids)
        self.assertEqual(['justin_littman'], tweet.mention_screen_names)

    def test_tweet_with_url(self):
        tweet = to_tweet(original_tweet_with_url, 'test')
        self.assertEqual(['http://www.gwu.edu/'], tweet.urls)

    def test_reply_tweet(self):
        tweet = to_tweet(reply_tweet, 'test')
        self.assertEqual('reply', tweet.tweet_type)
        self.assertEqual(['@justin_littman This is a test of replying to a tweet.'], tweet.text)

    def test_retweet(self):
        tweet = to_tweet(retweet, 'test')
        self.assertEqual('retweet', tweet.tweet_type)
        self.assertEqual(['RT @justin_littman: Ahh ... so in the context of web crawling, that\'s what a "frontier" '
                          'means: https://t.co/6oDZe03LsV'], tweet.text)

    def test_quote(self):
        tweet = to_tweet(quote_tweet, 'test')
        self.assertEqual('quote', tweet.tweet_type)
        self.assertEqual(['Test 10. Retweet. https://t.co/tBu6RRJoKr',
                          'First day at Gelman Library. First tweet. http://t.co/Gz5ybAD6os'], tweet.text)
        self.assertFalse(tweet.urls)

    def test_streaming_extended_tweet(self):
        tweet = to_tweet(streaming_extended_tweet, 'test')
        self.assertEqual(['@justin_littman Some of the changes went live. This is going to be an example for a blog '
                          'post I\'m writing that will be available at: https://t.co/MfQy5wTWBc'], tweet.text)

    def test_geotagged_tweet(self):
        tweet = to_tweet(geotagged_tweet, 'test')
        self.assertTrue(tweet.has_geo)
