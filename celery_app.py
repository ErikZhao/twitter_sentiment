# coding=utf-8

from celery import Celery
from twitter import twitter_operation
from sentiment import sentiment_analyze
from mongo import m

__author__ = "linghanzhao"
__created__ = "9/18/16"

"""
File Description
"""

app = Celery("celery_app")
app.conf.update(
    BROKER_URL="redis://localhost:6379",
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TIMEZONE='UTC',
    CELERYD_MAX_TASKS_PER_CHILD=100,
    CELERYD_HIJACK_ROOT_LOGGER=False,
    CELERYD_LOG_COLOR=True,
    CELERY_ROUTES={
        "celery_app.run_twitter_consumer": {"queue": "run_twitter_consumer"},
        "celery_app.run_twitter_sentiment": {"queue": "run_twitter_sentiment"}
    }
)

db = m.get_db()
collections = m.get_collections(db)


@app.task
def run_twitter_consumer():
    twitter_operation.tc_task(task=run_twitter_sentiment)


@app.task
def run_twitter_sentiment(id=None):
    sentiment_analyze.sentiment_task(id=id, collections=collections)

