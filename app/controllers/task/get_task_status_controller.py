from celery.result import AsyncResult
from fastapi import HTTPException

from app.config.celery_config import celery_app


async def fetch_task_status(task_id: str) -> dict:
    try:
        result = AsyncResult(
            task_id,
            app=celery_app
        )
        return {
            "task_id": task_id,
            "state": result.state,
            "result": result.info
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Something went wrong while fetching task status. Exited with exception: {e}"
        )