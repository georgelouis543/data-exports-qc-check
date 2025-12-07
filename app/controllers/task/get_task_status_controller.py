from celery.result import AsyncResult

from app.config.celery_config import celery_app


async def fetch_task_status(task_id: str):
    result = AsyncResult(
        task_id,
        app=celery_app
    )
    return {
        "task_id": task_id,
        "state": result.state,
        "result": result.info
    }