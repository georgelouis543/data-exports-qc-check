from typing import Optional, Any

from pydantic import BaseModel


class TaskStatusResponse(BaseModel):
    task_id: str
    state: str
    progress: str | None = None
    error: str | None = None
    result: Optional[Any] = None