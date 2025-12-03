from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.controllers.health.db_health_controller import db_health_check_handler
from app.schemas.health import HealthResponse

router = APIRouter(
    prefix="/health",
    tags=["health"]
)


@router.get("")
async def root() -> dict[str, str]:
    return {"message": "Health Check Endpoint"}


@router.get(
    "/db-health",
    summary="DB Health Check Endpoint",
    response_model=HealthResponse
)
async def db_health_check(
        session: AsyncSession = Depends(get_session)
) -> dict[str, str]:
    result = await db_health_check_handler(session)
    return result