from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def db_health_check_handler(
        session: AsyncSession
) -> dict:
    try:
        await session.execute(text("SELECT 1"))
        return {
            "status": "ok",
            "detail": "Database connection is healthy."
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database connection failed: {str(e)} | Service Unavailable."
        )