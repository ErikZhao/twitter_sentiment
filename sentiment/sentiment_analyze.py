# coding=utf-8

from textblob import TextBlob
from mongo import m
from bson.objectid import ObjectId

__author__ = "linghanzhao"
__created__ = "9/17/16"

"""
File Description
"""


def analyze_sentiment(text=None):
    """
    This function use textblob to analyze sentiment from text
    :param text:
    :return:
    """
    if not text:
        return 0

    try:
        testimonial = TextBlob(text)
        polarity = testimonial.sentiment.polarity
    except:
        polarity = 0
        print "get sentiment from tweets text failed"

    return polarity


def sentiment_task(id=None, collections=None):
    if not id or not collections:
        return

    # get sentiments and streaming collections from mongo
    sentiments_collection = collections['twitter_sentiments']
    streaming_collection = collections['twitter_streaming']

    # calculate polarity from tweets text and get coordinates from streaming collection
    streaming_doc = streaming_collection.find_one({'_id': ObjectId(id)})
    text = streaming_doc.get('text', '')
    coordinates = streaming_doc.get('coordinates', [])
    polarity = analyze_sentiment(text)

    # insert polarity and coordinates into sentiment collection
    sentiment_json = {"polarity": polarity, "coordinates": coordinates}
    sentiments_collection.insert_one(sentiment_json)


