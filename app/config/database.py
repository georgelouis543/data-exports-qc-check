import logging
import os
from typing import AsyncGenerator

from dotenv import load_dotenv
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession
)
from sqlalchemy.orm import declarative_base

load_dotenv()

DATABASE_URL = os.getenv("DB_PROD_URL")

# SQLAlchemy Base for model declarations
Base = declarative_base()

# Create the async engine
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_size=20,  # Larger base pool
    max_overflow=20,  # Allows up to 40 total
    pool_timeout=30,  # Wait up to 30s for a connection
)

# Create a session factory
async_session = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# Dependency for getting DB session
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    try:
        async with async_session() as session:
            # Ping the DB to ensure the connection is alive
            await session.execute(text("SELECT 1"))
            logging.info("Database connection established successfully")
            yield session

    except (
            SQLAlchemyError,
            OSError
    ) as e:
        logging.error(f"Database connection error: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Database temporarily unavailable. Exited with error {str(e)}"
        )