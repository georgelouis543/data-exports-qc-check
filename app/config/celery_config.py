import os

from celery import Celery
from dotenv import load_dotenv

load_dotenv()

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
CELERY_BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND_URL")

celery_app = Celery(
    "qc_tasks",
    broker=CELERY_BROKER_URL,
    backend=CELERY_BACKEND_URL,
    include=["app.tasks.qc_tasks"]
)


celery_app.conf.task_routes = {
    "qc_tasks.*": {
        "queue": "qc_queue"
    },
}