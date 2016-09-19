# coding=utf-8

from pymongo import MongoClient
import pymongo

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
    # twitter_streaming = db.twitter_streaming
    twitter_sentiments = db.twitter_sentiments

    collections = {
        # 'twitter_streaming': twitter_streaming,
        "twitter_sentiments": twitter_sentiments
    }
    return collections


def insert_one_document(collection=None, document=None):
    """This function inserts one document into the collection.

    :param collection: collection name.
    :type collection: pymongo.collection.Collection.
    :param document: json document.
    :type document: dict.
    :return id: inserted document ObjectId or -1 if insertion failed.
    :rtype id: ObjectId or int.

    """
    if collection is None or document is None:
        return -1

    try:
        id = collection.insert_one(document).inserted_id
    except:
        return -1

    return id


def update_one_document(collection=None, query=None, doc=None):
    """The function updates one document according to a certain query, and the entire content will be replace or added according to the field name.

    :param collection: collection name.
    :type collection: pymongo.collection.Collection.
    :param query: json format, especially with object id Sample: ({"_id":id}).
    :type query: dict.
    :param doc: the fields need to be update to the original document Sample: ({"field_name":value}).
    :return update_doc: updated count (should be one).
    :rtype update_doc: int

    """
    update_doc = None
    if collection is None:
        return update_doc

    if doc is None:
        return update_doc

    try:
        update_doc = collection.find_one_and_update(query, {"$set": doc},
                                                    return_document=pymongo.collection.ReturnDocument.AFTER)
    except:
        return update_doc


def get_many_documents(collection=None, query=None):
    """This function retrieves many documents with the same id under a certain query ({"field_name":value}).

    :param collection: collection name.
    :type collection: pymongo.collection.Collection.
    :param query: json format, Sample: ({"field_name":value}).
    :type query: dict.
    :return documents: many documents found by this query.
    :rtype documents: pymongo.command_cursor.CommandCursor.

    """
    if not collection or not query:
        return None

    try:
        # find result by query and get the cursor object
        documents = collection.find(query).limit(10000)
    except:
        return None

    return documents