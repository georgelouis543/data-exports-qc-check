from sqlalchemy.ext.asyncio import AsyncSession


async def verify_file_format_using_metadata(
        session: AsyncSession,
        extracted_file_path: str,
        task_id: str
):
    pass