# coding=utf-8

import flask
from flask import Flask, request
from mongo import m

__author__ = "linghanzhao"
__created__ = "9/18/16"

"""
File Description
"""

application = Flask(__name__)

db = m.get_db()
collections = m.get_collections(db)


@application.route("/")
def get_parameters():
    lat = request.args.get('latitude')
    lon = request.args.get('longitude')
    radius = request.args.get('radius')
    json = search_geo_mongo(lat, lon, radius)
    return flask.render_template('result.html', result=json)


def search_geo_mongo(lat=None, lon=None, radius=None):
    if not lat or not lon or not radius:
        return None

    s_json = {}
    b_json = {}
    polarity_avg = 0.0
    count = 0

    # geo query to get all documents cursor
    lat, lon, radius = int(lat), int(lon), int(radius)
    geo_query = {'loc':
                     {'$geoWithin':
                          {'$centerSphere':
                               [[lat, lon], radius / 3963.2]
                           }
                      }
                 }
    docs = m.get_many_documents(collections['twitter_sentiments'], geo_query)

    # get amount of tweets
    count = docs.count()

    # sort cursor and get most positive and most negative tweets
    biggest = docs.sort("polarity", -1).limit(1)
    for b in biggest:
        b_json = b
    docs = m.get_many_documents(collections['twitter_sentiments'], geo_query)
    smallest = docs.sort("polarity", 1).limit(1)
    for s in smallest:
        s_json = s

    # get polarity average
    avg_query = [
        {
            "$match": geo_query
        },
        {
            "$group": {
                "_id": None,
                "polarity_avg": {"$avg": "$polarity"},
            }
        }
    ]
    for avg in db.twitter_sentiments.aggregate(avg_query):
        polarity_avg = avg.get("polarity_avg", 0.0)

    # build result json
    json = {
        "tweets": count,
        "average_polarity": polarity_avg,
        "most_positive": {
            "text": b_json.get("text", ""),
            "coordinates": b_json.get("loc", {}).get("coordinates", []),
            "polarity": b_json.get("polarity", 0.0)
        },
        "most_negative": {
            "text": s_json.get("text", ""),
            "coordinates": s_json.get("loc", {}).get("coordinates", []),
            "polarity": s_json.get("polarity", 0.0)
        }
    }

    return json


if __name__ == "__main__":
    application.debug = True
    application.run(host='0.0.0.0')
