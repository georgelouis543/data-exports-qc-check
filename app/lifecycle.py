import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config.database import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("ENV", "development") != "production":
        await init_db()

    yield