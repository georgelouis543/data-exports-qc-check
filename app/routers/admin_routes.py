from fastapi import APIRouter

router = APIRouter(
    prefix="/admin",
    tags=["admin"]
)


@router.get("")
async def root() -> dict[str, str]:
    return {"message": "Admin Root"}