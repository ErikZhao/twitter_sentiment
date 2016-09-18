# coding=utf-8

from pymongo import MongoClient

__author__ = "linghanzhao"
__created__ = "9/17/16"

"""
File Description
"""

MONGOCLIENT = None


def get_client():
    """
    This function is to get the Mongo client
    :param host:
    :param user:
    :param password:
    :return:
    """

    global MONGOCLIENT

    if MONGOCLIENT is not None:
        return MONGOCLIENT

    MONGOCLIENT = MongoClient(host="localhost")
    # MONGOCLIENT = MongoClient(host="10.10.4.4")
    return MONGOCLIENT


def get_db(name='twitters'):  # twitters database
    """
    This function returns the database for the current application
    :param name:
    :return:
    """
    try:
        client = get_client()
        db = client.get_database(name)
    except Exception as e:
        print e
        return None
    return db


def get_collections(db=None):
    """This function returns all collections in database.

    :param db: database.
    :type db: pymongo.database.Database.
    :return collections: all collections in database.
    :rtype collections: dict

    """

    if db is None:
        return None

    # Collections name
    twitter_streaming = db.twitter_streaming
    twitter_sentiments = db.twitter_sentiments

    # collections (and databases) in MongoDB is that they are created lazily,
    # - none of the above commands have actually performed any operations on the MongoDB server.
    # Collections and databases are created when the first document is inserted into them.

    collections = {
        'twitter_streaming': twitter_streaming,
        "twitter_sentiments": twitter_sentiments
    }
    return collections


