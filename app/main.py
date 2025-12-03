from fastapi import FastAPI

from app.lifecycle import lifespan
from app.logging import configure_logging, LogLevels
from app.middleware.app_middleware import add_middlewares
from app.routers import health_routes, qc_check_routes, admin_routes

configure_logging(LogLevels.info)


app = FastAPI(lifespan=lifespan)

add_middlewares(app)

app.include_router(admin_routes.router)
app.include_router(health_routes.router)
app.include_router(qc_check_routes.router)

@app.get("/")
async def root() -> dict:
    return {
        "message": "Data Exports QC automation suite."
    }
