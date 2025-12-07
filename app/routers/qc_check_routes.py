from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.controllers.qc_check.metadata_validation_controller import metadata_validation_handler
from app.schemas.qc_check import QcCheckRequest, QcCheckResponse

router = APIRouter(
    prefix="/qc-check",
    tags=["qc-check"]
)


@router.get("")
async def root() -> dict[str, str]:
    """
    Root endpoint for QC Check module.
    """
    return {"message": "QC Check module is operational."}

@router.post(
    "/run-qc-check",
    response_model=QcCheckResponse,
)
async def run_qc_check(
        qc_check_request: QcCheckRequest,
        session: AsyncSession = Depends(get_session)
) -> QcCheckResponse:
    """
    Endpoint to run quality control checks on the provided data.
    """
    # Placeholder for QC check logic
    result = await metadata_validation_handler(
        session,
        qc_check_request
    )
    return result