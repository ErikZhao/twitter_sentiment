# coding=utf-8

import tweepy
from mongo import m

__author__ = "linghanzhao"
__created__ = "9/17/16"

"""
File Description
"""

consumer_key = 'WkUVCAmfBUdEUdf5R5fCF9Bi3'
consumer_token = 'Bc7waqqWAkVK9WLkXg3ghqU3hb6T4k0nyg4fSZtj2MnQxsMfic'

access_token = '753382617777512448-A0JXSUr0UCbm6gJB97BoPNsdvB5aWAJ'
access_token_secret = 'J0K2dOJPXIbbh39knKQ7W0XLX2HU2clmsPB9dBeJmQoOq'

auth = tweepy.OAuthHandler(consumer_key, consumer_token)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
stream_tweets_limit = 20

db = m.get_db()
collections = m.get_collections(db)


# override tweepy.StreamListener to add logic to on_status and error
class MyStreamListener(tweepy.StreamListener):
    def __init__(self, api=None):
        super(MyStreamListener, self).__init__()
        self.num_tweets = 0
        self.ids = []

    def on_status(self, status):
        # filter out tweets without geo and store others to twitter_sentiments collection
        if status.coordinates:
            if self.num_tweets < stream_tweets_limit:
                id = m.insert_one_document(collections['twitter_sentiments'], {"text": status.text, "coordinates": status.coordinates['coordinates']})
                self.ids.append(str(id))
                self.num_tweets += 1
                return True
            else:
                return False

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False


def twitter_consumer():
    """
    This function consume tweets on limited amounts
    :return:
    """
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    myStream.filter(locations=[-180, -90, 180, 90], languages=['en'])
    return myStreamListener.ids


def tc_task(task):
    ids = twitter_consumer()
    for id in ids:
        task.delay(str(id))
