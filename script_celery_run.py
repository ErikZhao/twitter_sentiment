from celery import Celery

app = Celery("celery_app")
app.conf.update(
        BROKER_URL="redis://localhost:6379",
        CELERY_TASK_SERIALIZER='json',
        CELERY_ACCEPT_CONTENT=['json'],
        CELERY_TIMEZONE='UTC',
        # CELERY_RDBSIG=1,
        CELERY_ROUTES={
            "celery_app.run_twitter_consumer": {"queue": "run_twitter_consumer"},
            "celery_app.run_twitter_sentiment": {"queue": "run_twitter_sentiment"}
        },
)
# celery -A celery_app worker -l info -Q celery,run_twitter_consumer,run_twitter_sentiment


@app.task
def run_twitter_consumer():
    pass


def get_cid(task=None):

    task.delay()


get_cid(run_twitter_consumer)