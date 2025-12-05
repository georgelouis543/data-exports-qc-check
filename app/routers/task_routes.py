from fastapi import APIRouter

from app.schemas.task import TaskStatusResponse

router = APIRouter(
    prefix="/tasks",
    tags=["tasks"]
)


@router.get("")
async def root() -> dict[str, str]:
    """
    Root endpoint for Monitoring Celery Tasks.
    """
    return {"message": "Celery Tasks module is operational."}


@router.get(
    "/status/{task_id}",
    response_model=TaskStatusResponse,
)
async def get_task_status(task_id: str) -> dict[str, str]:
    """
    Get the status of a Celery task by its ID.
    """
    # Placeholder implementation
    return {"state": "", "progress": "queued / waiting"}