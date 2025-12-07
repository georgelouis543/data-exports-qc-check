import base64
import json
import logging

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.qc_check import QcCheckRequest
from app.tasks.qc_tasks import metadata_validation_task


async def metadata_validation_handler(
        session: AsyncSession,
        qc_check_req_data: QcCheckRequest
):
    try:
        if not qc_check_req_data:
            logging.warning("No download data provided.")
            raise HTTPException(
                status_code=400,
                detail="No download data available."
            )

        feed_id = qc_check_req_data.metadata_feed_id
        if not feed_id:
            logging.warning("Metadata feed ID is missing.")
            raise HTTPException(
                status_code=400,
                detail="Metadata feed ID is required."
            )

        # Check if feed exists in the database
        stmt = (
            f"SELECT file_compression "
            f"FROM dataexports_api.lkp_metadata_feed_list "
            f"WHERE id = {feed_id}"
        )
        result = await session.execute(text(stmt))
        if not result.first():
            logging.warning(f"Metadata feed with ID {feed_id} does not exist.")
            raise HTTPException(
                status_code=404,
                detail="Metadata feed not found."
            )

        # Base64 decode
        decoded_bytes = base64.b64decode(qc_check_req_data.raw_download_data)
        decoded_download_data_bytes = decoded_bytes.decode('utf-8')

        download_data = json.loads(decoded_download_data_bytes)
        if not download_data or not isinstance(download_data, list):
            logging.warning("Download data is empty or not a list.")
            raise HTTPException(
                status_code=400,
                detail="No download data available."
            )

        download_url = download_data[0].get("download_url")
        file_name = download_data[0].get("file_name")

        if not download_url or not file_name:
            logging.warning("Missing download_url or file_name in the download data.")
            raise HTTPException(
                status_code=400,
                detail="Missing download_url or file_name"
            )

        # Add metadata validation task to the queue
        task = metadata_validation_task.delay(feed_id, download_data)
        logging.info(f"Metadata validation task queued with ID: {task.id}")

        return {
            "message": "Metadata validation task has been queued successfully.",
            "feed_id": feed_id,
            "task_id": task.id  # Placeholder task ID
        }

    except HTTPException as e:
        logging.error(f"HTTP Exception: {e}")
        raise e

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred during metadata validation."
        )
